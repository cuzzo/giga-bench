import assert from 'node:assert/strict';
import { test } from 'node:test';
import Engine from './engine.mjs';

// ---------------------------------------------------------------------------
// Mock fs implementing the documented synchronous storage semantics.
// ---------------------------------------------------------------------------

function enoent(name) {
  const err = new Error('ENOENT: ' + name);
  err.code = 'ENOENT';
  return err;
}

class MockFs {
  constructor() {
    this.files = new Map();          // live: name -> Buffer
    this.durableFiles = new Map();   // content made durable via sync({name})
    this.durableDir = new Map();     // directory made durable via sync()
    this.writeLimit = Infinity;      // simulate short writes
    this._fails = [];
    this._failsAfter = [];
  }

  toFs() {
    return (op, args) => this.dispatch(op, args);
  }

  dispatch(op, args = {}) {
    switch (op) {
      case 'list': return this.list();
      case 'read': return this.read(args);
      case 'write': return this.write(args);
      case 'truncate': return this.truncate(args);
      case 'rename': return this.rename(args);
      case 'unlink': return this.unlink(args);
      case 'sync': return this.sync(args);
      default: throw new Error('unknown fs op: ' + op);
    }
  }

  _fail(op) {
    for (let i = 0; i < this._fails.length; i++) {
      const failure = this._fails[i];
      if (failure.op === op) {
        this._fails.splice(i, 1);
        const err = new Error(op + ' failed');
        err.code = failure.code;
        throw err;
      }
    }
  }

  _failAfter(op) {
    for (let i = 0; i < this._failsAfter.length; i++) {
      const failure = this._failsAfter[i];
      if (failure.op === op) {
        this._failsAfter.splice(i, 1);
        const err = new Error(op + ' failed after effect');
        err.code = failure.code;
        throw err;
      }
    }
  }

  inject(op, code = 'EIO') {
    this._fails.push({ op, code });
  }

  // Raise an error after the operation has taken effect, modelling the
  // uncertain-I/O rule.
  injectAfter(op, code = 'EIO') {
    this._failsAfter.push({ op, code });
  }

  list() {
    this._fail('list');
    return Array.from(this.files.keys()).sort();
  }

  read({ name, offset = 0, size } = {}) {
    this._fail('read');
    if (!this.files.has(name)) throw enoent(name);
    const buf = this.files.get(name);
    if (offset > buf.length) return Buffer.alloc(0);
    const end = size === undefined ? buf.length : Math.min(buf.length, offset + size);
    return Buffer.from(buf.subarray(offset, end));
  }

  write({ name, offset = 0, data } = {}) {
    this._fail('write');
    data = Buffer.from(data);
    const existing = this.files.get(name);
    const toWrite = Math.min(data.length, this.writeLimit);
    const newLength = Math.max(existing ? existing.length : 0, offset + toWrite);
    const next = Buffer.alloc(newLength);
    if (existing) existing.copy(next, 0);
    data.subarray(0, toWrite).copy(next, offset);
    this.files.set(name, next);
    this._failAfter('write');
    return toWrite;
  }

  truncate({ name, size } = {}) {
    this._fail('truncate');
    if (!this.files.has(name)) throw enoent(name);
    const old = this.files.get(name);
    const next = Buffer.alloc(size);
    old.subarray(0, Math.min(old.length, size)).copy(next, 0);
    this.files.set(name, next);
  }

  rename({ name, to } = {}) {
    this._fail('rename');
    if (!this.files.has(name)) throw enoent(name);
    const data = this.files.get(name);
    this.files.set(to, data);
    this.files.delete(name);
  }

  unlink({ name } = {}) {
    this._fail('unlink');
    if (!this.files.has(name)) throw enoent(name);
    this.files.delete(name);
    this._failAfter('unlink');
  }

  sync(arg) {
    if (arg && typeof arg === 'object' && 'name' in arg) {
      this._fail('syncfile');
      if (!this.files.has(arg.name)) throw enoent(arg.name);
      this.durableFiles.set(arg.name, this.files.get(arg.name));
      this._failAfter('syncfile');
    } else {
      this._fail('sync');
      this.durableDir = new Map();
      for (const [name, buf] of this.files) {
        this.durableDir.set(name, buf);
      }
      this._failAfter('sync');
    }
  }

  powerLoss() {
    const surviving = new Map();
    for (const [name, buf] of this.durableDir) {
      surviving.set(name, this.durableFiles.has(name) ? this.durableFiles.get(name) : buf);
    }
    this.files = surviving;
    this.durableFiles = new Map(surviving);
    this.durableDir = new Map(surviving);
    this._fails = [];
    this._failsAfter = [];
  }

  totalBytes() {
    let total = 0;
    for (const buf of this.files.values()) total += buf.length;
    return total;
  }

  dataNames() {
    return this.list().filter((n) => /^data-\d+$/.test(n));
  }
}

function pending(seq) {
  return { seq, state: 'pending', attempts: 0, reason: null };
}

function cancelled(seq) {
  return { seq, state: 'cancelled', attempts: 0, reason: null };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test('put/get basic receipt and not_found', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: 1 } }), pending(1));
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'missing' }), { error: 'not_found' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 'other', key: 'k' }), { error: 'not_found' });
});

test('sequence allocation per stream', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  assert.equal(engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} }).seq, 1);
  assert.equal(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} }).seq, 2);
  assert.equal(engine.handle({ op: 'put', stream: 's', key: 'c', destination: 'd', payload: {} }).seq, 3);
  assert.equal(engine.handle({ op: 'put', stream: 't', key: 'a', destination: 'd', payload: {} }).seq, 1);
});

test('idempotent put ignores object member order', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const first = engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: 1, b: [1, 2, 3] } });
  assert.deepEqual(first, pending(1));
  const second = engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { b: [1, 2, 3], a: 1 } });
  assert.deepEqual(second, pending(1));
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: [1, 2, 3] } }), { error: 'conflict' });
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'other', payload: { a: 1, b: [1, 2, 3] } }), { error: 'conflict' });
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: 1, b: [3, 2, 1] } }), { error: 'conflict' });
});

test('cancel transitions and is idempotent', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  assert.deepEqual(engine.handle({ op: 'cancel', stream: 's', key: 'k' }), cancelled(1));
  assert.deepEqual(engine.handle({ op: 'cancel', stream: 's', key: 'k' }), cancelled(1));
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), cancelled(1));
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), cancelled(1));
  assert.deepEqual(engine.handle({ op: 'cancel', stream: 's', key: 'missing' }), { error: 'not_found' });
});

test('sequence numbers are not reused after cancellation', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'cancel', stream: 's', key: 'a' });
  assert.equal(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} }).seq, 2);
});

test('malformed and unknown requests are invalid without state change', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: 1 } });

  const invalidRequests = [
    null,
    undefined,
    42,
    'put',
    [],
    { op: 'nope' },
    { op: 'put' },
    { op: 'put', stream: '', key: 'k', destination: 'd', payload: {} },
    { op: 'put', stream: 's', key: '', destination: 'd', payload: {} },
    { op: 'put', stream: 's', key: 'k', destination: '', payload: {} },
    { op: 'put', stream: 1, key: 'k', destination: 'd', payload: {} },
    { op: 'put', stream: 's', key: 'k', destination: 'd', payload: null },
    { op: 'put', stream: 's', key: 'k', destination: 'd', payload: [] },
    { op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: undefined } },
    { op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: NaN } },
    { op: 'get' },
    { op: 'get', stream: 's' },
    { op: 'cancel', stream: 's' },
  ];

  for (const request of invalidRequests) {
    assert.deepEqual(engine.handle(request), { error: 'invalid' }, JSON.stringify(request));
  }

  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
});

test('extra request properties are ignored', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  assert.deepEqual(
    engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { a: 1 }, extra: true }),
    pending(1),
  );
});

test('replies and stored payloads do not share mutable state with caller', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const payload = { nested: { x: 1 } };
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload }), pending(1));
  payload.nested.x = 999;
  payload.added = true;
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { nested: { x: 1 } } }), pending(1));

  const receipt = engine.handle({ op: 'get', stream: 's', key: 'k' });
  receipt.seq = 999;
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
});

test('state survives a new instance on the same storage', () => {
  const fs = new MockFs();
  const first = new Engine(fs.toFs(), {});
  first.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });
  first.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } });
  first.handle({ op: 'cancel', stream: 's', key: 'a' });

  const second = new Engine(fs.toFs(), {});
  assert.deepEqual(second.handle({ op: 'get', stream: 's', key: 'a' }), cancelled(1));
  assert.deepEqual(second.handle({ op: 'get', stream: 's', key: 'b' }), pending(2));
  assert.equal(second.handle({ op: 'put', stream: 's', key: 'c', destination: 'd', payload: {} }).seq, 3);
});

test('committed events survive a power loss', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } });
  engine.handle({ op: 'cancel', stream: 's', key: 'a' });

  fs.powerLoss();

  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), cancelled(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'b' }), pending(2));
  assert.equal(recovered.handle({ op: 'put', stream: 's', key: 'c', destination: 'd', payload: {} }).seq, 3);
});

test('short writes are handled', () => {
  const fs = new MockFs();
  fs.writeLimit = 3;
  const engine = new Engine(fs.toFs(), {});
  const big = { text: 'x'.repeat(500), list: Array.from({ length: 50 }, (_, i) => i) };
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: big }), pending(1));
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));

  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
  assert.deepEqual(recovered.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: big }), pending(1));
});

test('zero-progress write reports io and can be retried', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.writeLimit = 0;
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { error: 'not_found' });

  fs.writeLimit = Infinity;
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), pending(1));
});

test('io error on content sync is reported and retry is consistent', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });

  fs.inject('syncfile');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } }), { error: 'io' });
  // The failed append may have committed; either way it must be consistent and durable now.
  const afterFailure = engine.handle({ op: 'get', stream: 's', key: 'b' });
  assert.ok(afterFailure.error === 'not_found' || afterFailure.seq === 2);

  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } }), pending(2));
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'b' }), pending(2));
});

test('io error on write is reported and retry is consistent', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });

  fs.inject('write');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { error: 'not_found' });

  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } }), pending(2));
});

test('cancelled event survives an interrupted later operation', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'cancel', stream: 's', key: 'a' });

  fs.inject('write');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} }), { error: 'io' });

  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), cancelled(1));
});

test('partial leftover snapshot files are ignored during recovery', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });

  // Simulate a torn leftover write with a higher generation number.
  fs.files.set('data-999', Buffer.from('this is not a valid snapshot'));

  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), pending(1));
  assert.equal(recovered.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} }).seq, 2);
});

test('compact preserves public event information and bounds bytes', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});

  const payload = { items: Array.from({ length: 20 }, (_, i) => ({ i, text: 'v' + i })) };
  for (let i = 0; i < 10; i++) {
    engine.handle({ op: 'put', stream: 'stream-' + (i % 3), key: 'k' + i, destination: 'd' + i, payload });
  }
  engine.handle({ op: 'cancel', stream: 'stream-0', key: 'k0' });

  const before = {};
  for (let i = 0; i < 10; i++) {
    before['k' + i] = engine.handle({ op: 'get', stream: 'stream-' + (i % 3), key: 'k' + i });
  }

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  const B = fs.totalBytes();
  assert.equal(fs.dataNames().length, 1);

  for (let i = 0; i < 10; i++) {
    assert.deepEqual(engine.handle({ op: 'get', stream: 'stream-' + (i % 3), key: 'k' + i }), before['k' + i]);
  }

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  const B2 = fs.totalBytes();
  assert.ok(B2 <= Math.max(65536, 2 * B), `B2=${B2} exceeds bound from B=${B}`);
  assert.equal(fs.dataNames().length, 1);

  // Repeated compaction with unchanged state must not grow storage.
  for (let i = 0; i < 5; i++) engine.handle({ op: 'compact' });
  assert.ok(fs.totalBytes() <= Math.max(65536, 2 * B));
});

test('compaction result survives power loss', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } });
  engine.handle({ op: 'cancel', stream: 's', key: 'a' });
  assert.deepEqual(engine.handle({ op: 'compact' }), {});

  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), cancelled(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'b' }), pending(2));
});

test('compact reports io but keeps committed state valid', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } });

  fs.inject('sync');
  assert.deepEqual(engine.handle({ op: 'compact' }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), pending(1));
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), pending(2));

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), pending(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'b' }), pending(2));
});

test('get has no state changes and does not rewrite storage', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const namesBefore = fs.list().join(',');
  const bytesBefore = fs.totalBytes();
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { error: 'not_found' });
  assert.equal(fs.list().join(','), namesBefore);
  assert.equal(fs.totalBytes(), bytesBefore);
});

test('options are accepted and reserved', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { someFutureOption: true });
  assert.deepEqual(engine.handle({ op: 'compact' }), {});
});

test('failed put does not commit and recovery resolves the retry consistently', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k1', destination: 'd', payload: {} });

  fs.inject('write');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k2', destination: 'd', payload: {} }), { error: 'io' });

  // Power loss keeps only the committed prefix; k2 never reached the file.
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k1' }), pending(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k2' }), { error: 'not_found' });

  // Retrying the identity allocates the same sequence number.
  assert.deepEqual(recovered.handle({ op: 'put', stream: 's', key: 'k2', destination: 'd', payload: {} }), pending(2));
});

test('sequence numbers continue after compaction', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'c', destination: 'd', payload: {} });
  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'd', destination: 'd', payload: {} }), pending(4));
  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'e', destination: 'd', payload: {} }), pending(5));
});

test('nested payload equality and conflict detection', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const payload = { z: [1, { b: true, a: null }], a: 'text', n: 3.5 };
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload }), pending(1));
  const reordered = { n: 3.5, a: 'text', z: [1, { a: null, b: true }] };
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: reordered }), pending(1));
  assert.deepEqual(
    engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { ...reordered, n: 3.6 } }),
    { error: 'conflict' },
  );
});

test('compaction does not disturb pending or cancelled states', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'p', destination: 'd', payload: { v: 1 } });
  engine.handle({ op: 'put', stream: 's', key: 'c', destination: 'd', payload: { v: 2 } });
  engine.handle({ op: 'cancel', stream: 's', key: 'c' });
  assert.deepEqual(engine.handle({ op: 'compact' }), {});

  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'p' }), pending(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'c' }), cancelled(2));
  assert.deepEqual(recovered.handle({ op: 'cancel', stream: 's', key: 'c' }), cancelled(2));
  assert.deepEqual(recovered.handle({ op: 'put', stream: 's', key: 'p', destination: 'd', payload: { v: 1 } }), pending(1));
});

test('payload keys such as __proto__ are treated as data', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const a = JSON.parse('{"__proto__": {"x": 1}, "normal": 2}');
  const b = JSON.parse('{"normal": 2, "__proto__": {"x": 1}}');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: a }), pending(1));
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: b }), pending(1));
  assert.deepEqual(
    engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: JSON.parse('{"__proto__": {"x": 2}}') }),
    { error: 'conflict' },
  );
  assert.equal({}.x, undefined);
});

// ---------------------------------------------------------------------------
// Request 2: ordered delivery
// ---------------------------------------------------------------------------

function attempt(stream, key, seq, destination, payload, attemptNumber) {
  return { stream, key, seq, attempt: attemptNumber, destination, payload };
}

test('step emits only the lowest nonterminal event and does not re-emit', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { maxInFlight: 4, perDestination: 2, leaseMs: 10 });
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } });

  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 10 }), [attempt('s', 'a', 1, 'd', { n: 1 }, 1)]);
  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });

  // Acking frees capacity and lets the next event in the stream be emitted in the same call.
  assert.deepEqual(
    engine.handle({ op: 'step', now: 100, results: [{ stream: 's', key: 'a', attempt: 1, code: 200 }], budget: 10 }),
    [attempt('s', 'b', 2, 'd', { n: 2 }, 1)],
  );
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'acked', attempts: 1, reason: null });
});

test('independent streams proceed independently', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { maxInFlight: 4, perDestination: 4 });
  engine.handle({ op: 'put', stream: 's1', key: 'k', destination: 'd', payload: { s: 1 } });
  engine.handle({ op: 'put', stream: 's2', key: 'k', destination: 'd', payload: { s: 2 } });
  engine.handle({ op: 'put', stream: 's3', key: 'k', destination: 'd', payload: { s: 3 } });

  const emitted = engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  assert.equal(emitted.length, 3);
  assert.deepEqual(
    emitted.map((a) => a.stream).sort(),
    ['s1', 's2', 's3'],
  );
});

test('budget, maxInFlight and perDestination limits are respected', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { maxInFlight: 2, perDestination: 5 });
  for (let i = 0; i < 4; i++) {
    engine.handle({ op: 'put', stream: 's' + i, key: 'k', destination: 'd' + i, payload: { i } });
  }
  assert.equal(engine.handle({ op: 'step', now: 1, results: [], budget: 0 }).length, 0);
  assert.equal(engine.handle({ op: 'step', now: 1, results: [], budget: 10 }).length, 2); // maxInFlight
  assert.equal(engine.handle({ op: 'step', now: 1, results: [], budget: 10 }).length, 0);

  const fs2 = new MockFs();
  const engine2 = new Engine(fs2.toFs(), { maxInFlight: 10, perDestination: 2 });
  for (let i = 0; i < 4; i++) {
    engine2.handle({ op: 'put', stream: 's' + i, key: 'k', destination: 'd', payload: { i } });
  }
  assert.equal(engine2.handle({ op: 'step', now: 1, results: [], budget: 10 }).length, 2); // perDestination
  assert.equal(engine2.handle({ op: 'step', now: 1, results: [], budget: 10 }).length, 0);
});

test('ack and dead transitions set state, attempts and reason', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });

  engine.handle({ op: 'step', now: 1, results: [], budget: 10 }); // a inflight
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 404 }], budget: 10 }); // a dead, b inflight
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'dead', attempts: 1, reason: 'http:404' });

  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'b', attempt: 1, code: 204 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'acked', attempts: 1, reason: null });

  // Boundary codes: 300 is dead, 499 is dead.
  engine.handle({ op: 'put', stream: 't', key: 'x', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 't', key: 'x', attempt: 1, code: 300 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 't', key: 'x' }), { seq: 1, state: 'dead', attempts: 1, reason: 'http:300' });

  engine.handle({ op: 'put', stream: 't', key: 'y', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 't', key: 'y', attempt: 1, code: 499 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 't', key: 'y' }), { seq: 2, state: 'dead', attempts: 1, reason: 'http:499' });
});

test('cancel of inflight is busy; cancel of terminal returns receipt', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'cancel', stream: 's', key: 'a' }), { error: 'busy' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });

  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 200 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'cancel', stream: 's', key: 'a' }), { seq: 1, state: 'acked', attempts: 1, reason: null });
});

test('identical replayed puts return the terminal receipt', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { v: 1 } });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 200 }], budget: 0 });

  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { v: 1 } }), {
    seq: 1,
    state: 'acked',
    attempts: 1,
    reason: null,
  });
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { v: 2 } }), { error: 'conflict' });
});

test('results are processed in order; unknown/noncurrent/terminal results are ignored', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 }); // attempt 1

  engine.handle({
    op: 'step',
    now: 1,
    results: [
      { stream: 'unknown', key: 'x', attempt: 1, code: 200 },
      { stream: 's', key: 'a', attempt: 2, code: 200 },
      { stream: 's', key: 'a', attempt: 1, code: 200 },
      { stream: 's', key: 'a', attempt: 1, code: 500 },
    ],
    budget: 0,
  });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'acked', attempts: 1, reason: null });
});

test('retryable outcomes schedule a retry deadline', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // attempt 1

  // Failure at now=1: delay = min(16, 4) = 4, deadline = 5.
  assert.deepEqual(
    engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 }),
    [],
  );
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });

  // Not ready before the deadline.
  assert.deepEqual(engine.handle({ op: 'step', now: 4, results: [], budget: 10 }), []);
  // Ready at the deadline: attempt 2 emitted.
  const second = engine.handle({ op: 'step', now: 5, results: [], budget: 10 });
  assert.deepEqual(second, [attempt('s', 'k', 1, 'd', {}, 2)]);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 2, reason: null });
});

test('step validates the whole request before applying any outcome', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 5, results: [], budget: 10 });

  const invalid = [
    { op: 'step' },
    { op: 'step', now: -1 },
    { op: 'step', now: 1.5 },
    { op: 'step', now: 1, budget: -1 },
    { op: 'step', now: 1, budget: 1.5 },
    { op: 'step', now: 1, jitterMs: -1 },
    { op: 'step', now: 1, results: 'x' },
    { op: 'step', now: 1, results: [null] },
    { op: 'step', now: 1, results: [{ stream: '', key: 'k', attempt: 1, code: 200 }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: '', attempt: 1, code: 200 }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 0, code: 200 }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 99 }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 600 }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 200.5 }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 'other' }] },
    { op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: null }] },
  ];
  for (const request of invalid) {
    assert.deepEqual(engine.handle(request), { error: 'invalid' }, JSON.stringify(request));
  }
  // The valid first result of a later-invalid array must not have been applied.
  assert.deepEqual(
    engine.handle({
      op: 'step',
      now: 5,
      results: [
        { stream: 's', key: 'a', attempt: 1, code: 200 },
        { stream: 's', key: 'a', attempt: 1, code: 999 },
      ],
      budget: 0,
    }),
    { error: 'invalid' },
  );
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });
});

test('step time must be nondecreasing and persists across reopen', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });

  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 0 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 99, results: [], budget: 0 }), { error: 'invalid' });
  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 0 }), []);

  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'step', now: 99, results: [], budget: 0 }), { error: 'invalid' });
  assert.deepEqual(recovered.handle({ op: 'step', now: 100, results: [], budget: 0 }), []);
});

test('unresolved attempts survive reopening', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: { n: 1 } });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: { n: 2 } });
  const emitted = engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  assert.deepEqual(emitted, [attempt('s', 'a', 1, 'd', { n: 1 }, 1)]);

  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });
  assert.deepEqual(recovered.handle({ op: 'step', now: 1, results: [], budget: 10 }), []);
  assert.deepEqual(
    recovered.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 200 }], budget: 10 }),
    [attempt('s', 'b', 2, 'd', { n: 2 }, 1)],
  );
});

test('compaction preserves inflight, terminal and pending work', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'c', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 }); // a inflight
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 200 }], budget: 10 }); // a acked, b inflight
  engine.handle({ op: 'cancel', stream: 's', key: 'c' });

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'acked', attempts: 1, reason: null });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'inflight', attempts: 1, reason: null });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'c' }), { seq: 3, state: 'cancelled', attempts: 0, reason: null });
});

test('compaction stays within the 65536 + 8R byte bound', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const accepted = [];
  for (let i = 0; i < 200; i++) {
    const stream = 's' + (i % 5);
    const key = 'k' + i;
    const destination = 'd' + (i % 3);
    const payload = { i, text: 'value-' + i, nested: { list: [i, i + 1, i + 2] } };
    engine.handle({ op: 'put', stream, key, destination, payload });
    accepted.push({ stream, key, destination, payload });
  }
  const emitted = engine.handle({ op: 'step', now: 1, results: [], budget: 50 });
  engine.handle({
    op: 'step',
    now: 1,
    results: emitted.map((a) => ({ stream: a.stream, key: a.key, attempt: a.attempt, code: 500 })),
    budget: 0,
  });

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  const bytes = fs.totalBytes();
  let R = 0;
  for (const item of accepted) {
    const receipt = engine.handle({ op: 'get', stream: item.stream, key: item.key });
    R += Buffer.byteLength(
      JSON.stringify({
        stream: item.stream,
        key: item.key,
        destination: item.destination,
        payload: item.payload,
        seq: receipt.seq,
        state: receipt.state,
        attempts: receipt.attempts,
        reason: receipt.reason,
      }),
      'utf8',
    );
  }
  assert.ok(bytes <= 65536 + 8 * R, `bytes=${bytes} bound=${65536 + 8 * R}`);
});

test('sustained ingestion and delivery at scale', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { maxInFlight: 8, perDestination: 4 });
  const total = 2000;
  const streams = 50;
  for (let i = 0; i < total; i++) {
    engine.handle({ op: 'put', stream: 's' + (i % streams), key: 'k' + i, destination: 'd' + (i % 7), payload: { i } });
  }

  let emitted = 0;
  let now = 1;
  while (emitted < total && now < 10000) {
    const attempts = engine.handle({ op: 'step', now, results: [], budget: 100 });
    if (attempts.length === 0) {
      now++;
      continue;
    }
    emitted += attempts.length;
    const results = attempts.map((a) => ({ stream: a.stream, key: a.key, attempt: a.attempt, code: 200 }));
    engine.handle({ op: 'step', now, results, budget: 0 });
    now++;
  }
  assert.equal(emitted, total);

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.equal(recovered.handle({ op: 'get', stream: 's0', key: 'k0' }).state, 'acked');
  assert.equal(recovered.handle({ op: 'get', stream: 's' + (streams - 1), key: 'k' + (total - 1) }).state, 'acked');
});

test('a failed recovery rejects subsequent requests with io', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });

  // The append fails, and recovery's re-read also fails.
  fs.inject('syncfile');
  fs.inject('read');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'compact' }), { error: 'io' });

  // Reopening recovers previously confirmed work.
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), pending(1));
});

// ---------------------------------------------------------------------------
// Request 3: delivery timing and recovery
// ---------------------------------------------------------------------------

test('lease expiry is a retryable failure using the lease deadline as failure time', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { v: 1 } });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', { v: 1 }, 1)]);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });

  // At now=100 the lease (deadline 10) expires; failure time is 10, delay 4, so
  // the retry deadline 14 is already past and attempt 2 is emitted in the same call.
  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', { v: 1 }, 2)]);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 2, reason: null });
});

test('supplied result retry uses now as failure time and grows the delay with n', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // attempt 1

  // n=1 -> delay 4, failure at now=1 -> deadline 5.
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(engine.handle({ op: 'step', now: 4, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)]);

  // n=2 -> delay 8, failure at now=6 -> deadline 14.
  engine.handle({ op: 'step', now: 6, results: [{ stream: 's', key: 'k', attempt: 2, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'step', now: 13, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 14, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 3)]);
});

test('429 uses the maximum of the formula and a valid retryAfterMs', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });

  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 429, retryAfterMs: 20 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'step', now: 20, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 21, results: [], budget: 10 }), [attempt('s', 'a', 1, 'd', {}, 2)]);

  // retryAfterMs smaller than the formula is ignored in favour of the formula.
  engine.handle({ op: 'put', stream: 's2', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 21, results: [], budget: 10 }); // b attempt 1
  engine.handle({ op: 'step', now: 22, results: [{ stream: 's2', key: 'b', attempt: 1, code: 429, retryAfterMs: 2 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'step', now: 25, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 26, results: [], budget: 10 }), [attempt('s2', 'b', 1, 'd', {}, 2)]);
});

test('invalid retryAfterMs is ignored and jitter is applied to the formula', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });

  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  // Invalid retryAfterMs (not a nonnegative safe integer) is ignored; jitterMs=3 -> delay 7.
  engine.handle({
    op: 'step',
    now: 1,
    results: [{ stream: 's', key: 'a', attempt: 1, code: 429, retryAfterMs: 1.5 }],
    budget: 10,
    jitterMs: 3,
  });
  assert.deepEqual(engine.handle({ op: 'step', now: 7, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 8, results: [], budget: 10 }), [attempt('s', 'a', 1, 'd', {}, 2)]);

  // retryAfterMs on a non-429 code is ignored.
  engine.handle({ op: 'put', stream: 's2', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 8, results: [], budget: 10 }); // b attempt 1
  engine.handle({ op: 'step', now: 9, results: [{ stream: 's2', key: 'b', attempt: 1, code: 500, retryAfterMs: 100 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'step', now: 12, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 13, results: [], budget: 10 }), [attempt('s2', 'b', 1, 'd', {}, 2)]);
});

test('jitterMs greater than baseMs is invalid', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { baseMs: 4 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [], budget: 10, jitterMs: 5 }), { error: 'invalid' });
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [], budget: 10, jitterMs: -1 }), { error: 'invalid' });
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [], budget: 10, jitterMs: 4 }), [attempt('s', 'k', 1, 'd', {}, 1)]);
});

test('a retryable failure at maxAttempts becomes dead/exhausted', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 2 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // attempt 1
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)]);
  engine.handle({ op: 'step', now: 6, results: [{ stream: 's', key: 'k', attempt: 2, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'dead', attempts: 2, reason: 'exhausted' });
  // There is no further retry.
  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), {
    seq: 1,
    state: 'dead',
    attempts: 2,
    reason: 'exhausted',
  });
});

test('an expired attempt ignores later outcomes', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // attempt 1, lease 10

  // The result arrives after the lease expired in the same step and must be ignored.
  assert.deepEqual(
    engine.handle({ op: 'step', now: 10, results: [{ stream: 's', key: 'k', attempt: 1, code: 200 }], budget: 10 }),
    [],
  );
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
});

test('a waiting retry blocks later stream events and can be cancelled', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // a attempt 1
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 500 }], budget: 10 }); // a retry deadline 5

  assert.deepEqual(engine.handle({ op: 'step', now: 4, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'a', 1, 'd', {}, 2)]);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'pending', attempts: 0, reason: null });

  // Cancel the in-flight retry: a becomes terminal and b advances.
  engine.handle({ op: 'step', now: 5, results: [{ stream: 's', key: 'a', attempt: 2, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'cancel', stream: 's', key: 'a' }), { seq: 1, state: 'cancelled', attempts: 2, reason: null });
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'b', 2, 'd', {}, 1)]);
});

test('retry deadlines are stored, not recalculated', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10, jitterMs: 3 }); // deadline 8

  // A later step with different jitter must not change the stored deadline.
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10, jitterMs: 0 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 8, results: [], budget: 10, jitterMs: 0 }), [attempt('s', 'k', 1, 'd', {}, 2)]);
});

test('lease and retry deadlines survive reopening', () => {
  const leaseFs = new MockFs();
  const leaseFirst = new Engine(leaseFs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  leaseFirst.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  leaseFirst.handle({ op: 'step', now: 0, results: [], budget: 10 }); // lease 10
  leaseFs.powerLoss();
  const leaseEngine = new Engine(leaseFs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(leaseEngine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });
  assert.deepEqual(leaseEngine.handle({ op: 'step', now: 9, results: [], budget: 10 }), []);
  assert.deepEqual(leaseEngine.handle({ op: 'step', now: 10, results: [], budget: 10 }), []);
  assert.deepEqual(leaseEngine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(leaseEngine.handle({ op: 'step', now: 14, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)]);

  const retryFs = new MockFs();
  const retryFirst = new Engine(retryFs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  retryFirst.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  retryFirst.handle({ op: 'step', now: 0, results: [], budget: 10 });
  retryFirst.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 }); // deadline 5
  retryFs.powerLoss();
  const retryEngine = new Engine(retryFs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(retryEngine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(retryEngine.handle({ op: 'step', now: 4, results: [], budget: 10 }), []);
  assert.deepEqual(retryEngine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)]);
});

test('retryable codes are 100-199, 429, 500-599 and the failure strings', () => {
  const codes = [100, 199, 429, 500, 599, 'timeout', 'reset', 'malformed'];
  codes.forEach((code, i) => {
    const fs = new MockFs();
    const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });
    engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
    engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
    engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code }], budget: 10 });
    assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null }, JSON.stringify(code));
    assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)], JSON.stringify(code));
  });
});

test('attempt identity is stable across retries', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'dest', payload: { n: 7 } });
  const first = engine.handle({ op: 'step', now: 0, results: [], budget: 10 })[0];
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 'timeout' }], budget: 10 });
  const retry = engine.handle({ op: 'step', now: 5, results: [], budget: 10 })[0];
  assert.equal(retry.stream, first.stream);
  assert.equal(retry.key, first.key);
  assert.equal(retry.seq, first.seq);
  assert.equal(retry.destination, first.destination);
  assert.deepEqual(retry.payload, first.payload);
  assert.equal(retry.attempt, first.attempt + 1);
});

test('compaction preserves leases, retry deadlines and attempt numbers', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 'la', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 'rb', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // both attempt 1, lease 10
  engine.handle({ op: 'step', now: 1, results: [{ stream: 'rb', key: 'k', attempt: 1, code: 500 }], budget: 10 }); // rb deadline 5

  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 'la', key: 'k' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 'rb', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  // rb's retry deadline (5) has passed, so it is emitted; la's lease (10) has not expired.
  assert.deepEqual(recovered.handle({ op: 'step', now: 9, results: [], budget: 10 }), [attempt('rb', 'k', 1, 'd', {}, 2)]);
  // la expires at 10 (failure time 10, deadline 14).
  assert.deepEqual(recovered.handle({ op: 'step', now: 10, results: [], budget: 10 }), []);
  assert.deepEqual(recovered.handle({ op: 'step', now: 14, results: [], budget: 10 }), [attempt('la', 'k', 1, 'd', {}, 2)]);
});

test('lease expiry at maxAttempts exhausts the event', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 1 });
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // a attempt 1, lease 10

  // a's lease expires at 10; attempt 1 is the max, so a becomes dead/exhausted and b advances.
  assert.deepEqual(engine.handle({ op: 'step', now: 10, results: [], budget: 10 }), [attempt('s', 'b', 2, 'd', {}, 1)]);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'dead', attempts: 1, reason: 'exhausted' });

  // A late outcome for the expired attempt has no effect, and the state persists.
  engine.handle({ op: 'step', now: 10, results: [{ stream: 's', key: 'a', attempt: 1, code: 200 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'dead', attempts: 1, reason: 'exhausted' });
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 1 });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'a' }), { seq: 1, state: 'dead', attempts: 1, reason: 'exhausted' });
});

test('repeated retries eventually succeed with increasing attempt numbers', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 5 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 1)]);
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 }); // deadline 5
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)]);
  engine.handle({ op: 'step', now: 6, results: [{ stream: 's', key: 'k', attempt: 2, code: 'timeout' }], budget: 10 }); // deadline 14
  assert.deepEqual(engine.handle({ op: 'step', now: 14, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 3)]);
  engine.handle({ op: 'step', now: 14, results: [{ stream: 's', key: 'k', attempt: 3, code: 200 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'acked', attempts: 3, reason: null });
});

test('an interrupted step with a retry recovers consistently', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // attempt 1
  fs.inject('syncfile');
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 }), { error: 'io' });

  // Recovery adopts the written retry record and re-syncs it.
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(recovered.handle({ op: 'step', now: 5, results: [], budget: 10 }), [attempt('s', 'k', 1, 'd', {}, 2)]);
});

test('a retry deadline beyond MAX_SAFE_INTEGER survives reopen (record path)', () => {
  const fs = new MockFs();
  const big = Number.MAX_SAFE_INTEGER - 100;
  const first = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  first.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  first.handle({ op: 'step', now: big, results: [], budget: 10 });
  // 429 with a retryAfterMs that makes the deadline overflow the safe range.
  first.handle({ op: 'step', now: big, results: [{ stream: 's', key: 'k', attempt: 1, code: 429, retryAfterMs: 1000 }], budget: 10 });
  assert.deepEqual(first.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });

  fs.powerLoss();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  // No safe `now` reaches the stored deadline, so the retry must not fire early.
  assert.deepEqual(engine.handle({ op: 'step', now: big, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: Number.MAX_SAFE_INTEGER, results: [], budget: 10 }), []);
});

test('a retry deadline beyond MAX_SAFE_INTEGER survives compaction', () => {
  const fs = new MockFs();
  const big = Number.MAX_SAFE_INTEGER - 100;
  const first = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  first.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  first.handle({ op: 'step', now: big, results: [], budget: 10 });
  first.handle({ op: 'step', now: big, results: [{ stream: 's', key: 'k', attempt: 1, code: 429, retryAfterMs: 1000 }], budget: 10 });
  assert.deepEqual(first.handle({ op: 'compact' }), {});

  fs.powerLoss();
  const engine = new Engine(fs.toFs(), { leaseMs: 10, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(engine.handle({ op: 'step', now: Number.MAX_SAFE_INTEGER, results: [], budget: 10 }), []);
});

test('a lease deadline beyond MAX_SAFE_INTEGER does not expire on reopen', () => {
  const fs = new MockFs();
  const big = Number.MAX_SAFE_INTEGER - 100;
  const first = new Engine(fs.toFs(), { leaseMs: 1000, baseMs: 4, capMs: 16, maxAttempts: 3 });
  first.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  first.handle({ op: 'step', now: big, results: [], budget: 10 }); // lease = big + 1000 (unsafe)

  fs.powerLoss();
  const engine = new Engine(fs.toFs(), { leaseMs: 1000, baseMs: 4, capMs: 16, maxAttempts: 3 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });
  // The deadline is beyond any safe now, so the lease must not be treated as expired.
  assert.deepEqual(engine.handle({ op: 'step', now: Number.MAX_SAFE_INTEGER, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 1, reason: null });
});

// ---------------------------------------------------------------------------
// Request 4: operator controls
// ---------------------------------------------------------------------------

test('pause and resume return the documented replies', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd' }), { destination: 'd', paused: true });
  assert.deepEqual(engine.handle({ op: 'resume', destination: 'd' }), { destination: 'd', paused: false });
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd', until: null }), { destination: 'd', paused: true });
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd', until: 100 }), { destination: 'd', paused: true });
  assert.deepEqual(engine.handle({ op: 'resume', destination: 'd' }), { destination: 'd', paused: false });
});

test('malformed pause and resume requests are invalid', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const invalid = [
    { op: 'pause' },
    { op: 'pause', destination: '' },
    { op: 'pause', destination: 1 },
    { op: 'pause', destination: 'd', until: -1 },
    { op: 'pause', destination: 'd', until: 1.5 },
    { op: 'pause', destination: 'd', until: 'x' },
    { op: 'resume' },
    { op: 'resume', destination: '' },
    { op: 'resume', destination: 1 },
  ];
  for (const request of invalid) {
    assert.deepEqual(engine.handle(request), { error: 'invalid' }, JSON.stringify(request));
  }
  assert.deepEqual(engine.handle({ op: 'stats' }).destinations, {});
});

test('pause is idempotent and replaces an earlier pause', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd' }), { destination: 'd', paused: true });
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd' }), { destination: 'd', paused: true });
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd', until: 100 }), { destination: 'd', paused: true });
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd', until: null }), { destination: 'd', paused: true });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1000, results: [], budget: 10 });
  assert.equal(engine.handle({ op: 'stats' }).destinations.d.paused, true);
});

test('a paused destination receives no attempts, including retries', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'pause', destination: 'd' });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }), []);
  engine.handle({ op: 'resume', destination: 'd' });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }).map((a) => [a.stream, a.attempt]), [['s', 1]]);
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  engine.handle({ op: 'pause', destination: 'd' });
  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 10 }), []);
  engine.handle({ op: 'resume', destination: 'd' });
  assert.deepEqual(engine.handle({ op: 'step', now: 100, results: [], budget: 10 }).map((a) => [a.stream, a.attempt]), [['s', 2]]);
});

test('a paused destination with an in-flight attempt still consumes capacity', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { maxInFlight: 1, perDestination: 5 });
  engine.handle({ op: 'put', stream: 's1', key: 'k', destination: 'd1', payload: {} });
  engine.handle({ op: 'put', stream: 's2', key: 'k', destination: 'd2', payload: {} });
  const first = engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  const inflight = first[0];
  engine.handle({ op: 'pause', destination: inflight.destination });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }), []);
});

test('a timed pause is removed by a valid step at its end time even with budget 0', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'pause', destination: 'd', until: 5 });
  assert.deepEqual(engine.handle({ op: 'step', now: 4, results: [], budget: 0 }), []);
  assert.equal(engine.handle({ op: 'stats' }).destinations.d.paused, true);
  engine.handle({ op: 'step', now: 5, results: [], budget: 0 });
  assert.equal(engine.handle({ op: 'stats' }).destinations.d.paused, false);
  assert.deepEqual(engine.handle({ op: 'step', now: 5, results: [], budget: 10 }).map((a) => a.stream), ['s']);
});

test('other eligible streams run while a paused destination is blocked', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's1', key: 'k', destination: 'd1', payload: {} });
  engine.handle({ op: 'put', stream: 's2', key: 'k', destination: 'd2', payload: {} });
  engine.handle({ op: 'pause', destination: 'd1' });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }).map((a) => a.stream), ['s2']);
});

test('a paused pending head blocks later events in its stream', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'pause', destination: 'd' });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }), []);
  engine.handle({ op: 'resume', destination: 'd' });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }).map((a) => a.key), ['a']);
});

test('pause is allowed before a destination has events and survives reopen', () => {
  const fs = new MockFs();
  const first = new Engine(fs.toFs(), {});
  assert.deepEqual(first.handle({ op: 'pause', destination: 'future', until: 50 }), { destination: 'future', paused: true });
  assert.equal(first.handle({ op: 'stats' }).destinations.future.paused, true);
  fs.powerLoss();
  const engine = new Engine(fs.toFs(), {});
  assert.equal(engine.handle({ op: 'stats' }).destinations.future.paused, true);
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'future', payload: {} });
  assert.deepEqual(engine.handle({ op: 'step', now: 0, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 50, results: [], budget: 10 }).map((a) => a.stream), ['s']);
});

test('requeue returns a dead event to pending with its lifetime attempts', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { v: 1 } });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'dead', attempts: 1, reason: 'exhausted' });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [], budget: 10 }).map((a) => [a.stream, a.attempt]), [['s', 2]]);
});

test('requeue checks not_found, not_dead, busy and invalid in order', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100 });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { error: 'not_found' });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: '', key: 'k' }), { error: 'invalid' });
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'a' }), { error: 'not_dead' });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'a' }), { error: 'not_dead' });

  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 404 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'inflight', attempts: 1, reason: null });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'a' }), { error: 'busy' });
});

test('requeue starts a new maxAttempts allowance with first-attempt backoff', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 2 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  engine.handle({ op: 'step', now: 5, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 6, results: [{ stream: 's', key: 'k', attempt: 2, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'dead', attempts: 2, reason: 'exhausted' });

  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 2, reason: null });
  engine.handle({ op: 'step', now: 6, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 7, results: [{ stream: 's', key: 'k', attempt: 3, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 3, reason: null });
  assert.deepEqual(engine.handle({ op: 'step', now: 10, results: [], budget: 10 }), []);
  assert.deepEqual(engine.handle({ op: 'step', now: 11, results: [], budget: 10 }).map((a) => a.attempt), [4]);
});

test('requeue after a permanent outcome works and old outcomes stay stale', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, maxInFlight: 10 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 404 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 200 }], budget: 10 }); // stale
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'inflight', attempts: 2, reason: null });
});

test('a requeued event re-enters stream ordering at its original sequence', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, maxInFlight: 10 });
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 404 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'pending', attempts: 0, reason: null });
  engine.handle({ op: 'requeue', stream: 's', key: 'a' });
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [], budget: 10 }).map((a) => [a.key, a.attempt]), [['a', 2]]);
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'pending', attempts: 0, reason: null });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 2, code: 200 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'b' }), { seq: 2, state: 'inflight', attempts: 1, reason: null });
});

test('requeue survives reopen and compaction', () => {
  const fs = new MockFs();
  const first = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  first.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  first.handle({ op: 'step', now: 0, results: [], budget: 10 });
  first.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 }); // dead
  first.handle({ op: 'requeue', stream: 's', key: 'k' });
  assert.deepEqual(first.handle({ op: 'compact' }), {});
  fs.powerLoss();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  assert.deepEqual(engine.handle({ op: 'step', now: 1, results: [], budget: 10 }).map((a) => a.attempt), [2]);
});

test('stats reports states, streams and destinations', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, maxInFlight: 10, perDestination: 10 });
  engine.handle({ op: 'put', stream: 's1', key: 'a', destination: 'd1', payload: {} });
  engine.handle({ op: 'put', stream: 's1', key: 'b', destination: 'd1', payload: {} });
  engine.handle({ op: 'put', stream: 's2', key: 'a', destination: 'd2', payload: {} });
  engine.handle({ op: 'put', stream: 's3', key: 'a', destination: 'd3', payload: {} });
  engine.handle({ op: 'cancel', stream: 's2', key: 'a' }); // cancelled while pending
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 }); // s1/a, s3/a inflight
  engine.handle({ op: 'step', now: 0, results: [{ stream: 's1', key: 'a', attempt: 1, code: 200 }], budget: 10 }); // s1/a acked, s1/b inflight
  engine.handle({ op: 'step', now: 0, results: [{ stream: 's3', key: 'a', attempt: 1, code: 404 }], budget: 0 }); // dead
  engine.handle({ op: 'pause', destination: 'd2' });

  assert.deepEqual(engine.handle({ op: 'stats' }), {
    states: { pending: 0, inflight: 1, acked: 1, dead: 1, cancelled: 1 },
    streams: {
      s1: { head: 2, backlog: 1, acked: 1 },
      s2: { head: null, backlog: 0, acked: 0 },
      s3: { head: null, backlog: 0, acked: 0 },
    },
    destinations: {
      d1: { pending: 0, inflight: 1, paused: false },
      d2: { pending: 0, inflight: 0, paused: true },
      d3: { pending: 0, inflight: 0, paused: false },
    },
  });
});

test('stats counts pending retries and blocked heads', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 3 });
  engine.handle({ op: 'put', stream: 's', key: 'a', destination: 'd', payload: {} });
  engine.handle({ op: 'put', stream: 's', key: 'b', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'a', attempt: 1, code: 500 }], budget: 0 });
  assert.deepEqual(engine.handle({ op: 'stats' }), {
    states: { pending: 2, inflight: 0, acked: 0, dead: 0, cancelled: 0 },
    streams: { s: { head: 1, backlog: 2, acked: 0 } },
    destinations: { d: { pending: 2, inflight: 0, paused: false } },
  });
});

test('stats handles names matching JavaScript object properties', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: '__proto__', key: 'constructor', destination: 'toString', payload: {} });
  engine.handle({ op: 'pause', destination: 'hasOwnProperty', until: 10 });
  const stats = engine.handle({ op: 'stats' });
  assert.equal(Object.prototype.hasOwnProperty.call(stats.streams, '__proto__'), true);
  assert.deepEqual(stats.streams['__proto__'], { head: 1, backlog: 1, acked: 0 });
  assert.deepEqual(stats.destinations['toString'], { pending: 1, inflight: 0, paused: false });
  assert.deepEqual(stats.destinations['hasOwnProperty'], { pending: 0, inflight: 0, paused: true });
  assert.deepEqual(stats.states, { pending: 1, inflight: 0, acked: 0, dead: 0, cancelled: 0 });
});

test('stats does not mutate storage and returns fresh objects', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  const names = fs.list().join(',');
  const bytes = fs.totalBytes();
  const a = engine.handle({ op: 'stats' });
  const b = engine.handle({ op: 'stats' });
  assert.deepEqual(a, b);
  for (const object of [a, a.states, a.streams, a.streams.s, a.destinations, a.destinations.d]) {
    assert.notEqual(object, undefined);
  }
  assert.notEqual(a, b);
  assert.notEqual(a.states, b.states);
  assert.notEqual(a.streams, b.streams);
  assert.notEqual(a.streams.s, b.streams.s);
  assert.notEqual(a.destinations, b.destinations);
  assert.notEqual(a.destinations.d, b.destinations.d);
  assert.equal(fs.list().join(','), names);
  assert.equal(fs.totalBytes(), bytes);
  a.states.pending = 999;
  a.streams.s.head = 999;
  a.destinations.d.paused = true;
  assert.deepEqual(engine.handle({ op: 'stats' }), b);
});

test('an interrupted pause append recovers consistently', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.inject('syncfile');
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd', until: 10 }), { error: 'io' });
  assert.equal(engine.handle({ op: 'stats' }).destinations.d.paused, true);
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.equal(recovered.handle({ op: 'stats' }).destinations.d.paused, true);
});

test('an interrupted requeue append recovers consistently', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  fs.inject('syncfile');
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
});

test('compaction stays within 65536 + 8*(R + P) with pauses', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  const accepted = [];
  for (let i = 0; i < 100; i++) {
    const stream = 's' + (i % 5);
    const key = 'k' + i;
    const destination = 'd' + (i % 3);
    const payload = { i, text: 'value-' + i };
    engine.handle({ op: 'put', stream, key, destination, payload });
    accepted.push({ stream, key, destination, payload });
  }
  const pauses = [['d0', null], ['d1', 123456], ['d2', 999999]];
  for (const [destination, until] of pauses) {
    engine.handle({ op: 'pause', destination, until });
  }
  assert.deepEqual(engine.handle({ op: 'compact' }), {});
  const bytes = fs.totalBytes();
  let R = 0;
  for (const item of accepted) {
    const receipt = engine.handle({ op: 'get', stream: item.stream, key: item.key });
    R += Buffer.byteLength(
      JSON.stringify({
        stream: item.stream,
        key: item.key,
        destination: item.destination,
        payload: item.payload,
        seq: receipt.seq,
        state: receipt.state,
        attempts: receipt.attempts,
        reason: receipt.reason,
      }),
      'utf8',
    );
  }
  let P = 0;
  for (const [destination, until] of pauses) {
    P += Buffer.byteLength(JSON.stringify({ destination, until }), 'utf8');
  }
  assert.ok(bytes <= 65536 + 8 * (R + P), `bytes=${bytes} bound=${65536 + 8 * (R + P)}`);
});

test('repeated compaction with unchanged pause state stays bounded', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: { v: 1 } });
  engine.handle({ op: 'pause', destination: 'd', until: 999999 });
  engine.handle({ op: 'compact' });
  const B = fs.totalBytes();
  for (let i = 0; i < 5; i++) engine.handle({ op: 'compact' });
  assert.ok(fs.totalBytes() <= Math.max(65536, 2 * B), `bytes=${fs.totalBytes()} B=${B}`);
});

test('pause and requeue are rejected after an unrecoverable I/O error', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.inject('syncfile');
  fs.inject('read');
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd' }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd' }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'stats' }), { error: 'io' });
});

test('stats is rejected after an unrecoverable I/O error', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.inject('syncfile');
  fs.inject('read');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'stats' }), { error: 'io' });
});

test('reopening preserves stats', () => {
  const fs = new MockFs();
  const first = new Engine(fs.toFs(), { leaseMs: 100, maxInFlight: 10, perDestination: 10 });
  first.handle({ op: 'put', stream: 's1', key: 'a', destination: 'd1', payload: {} });
  first.handle({ op: 'put', stream: 's2', key: 'a', destination: 'd2', payload: {} });
  first.handle({ op: 'step', now: 0, results: [], budget: 10 });
  first.handle({ op: 'cancel', stream: 's2', key: 'a' });
  first.handle({ op: 'pause', destination: 'd3', until: 500 });
  const before = first.handle({ op: 'stats' });
  fs.powerLoss();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, maxInFlight: 10, perDestination: 10 });
  assert.deepEqual(engine.handle({ op: 'stats' }), before);
});

// ---------------------------------------------------------------------------
// Request 4: uncertain I/O
// ---------------------------------------------------------------------------

test('a write that takes effect then fails is recovered (put)', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.injectAfter('write');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
});

test('a content sync that takes effect then fails is recovered (put)', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.injectAfter('syncfile');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k' }), pending(1));
});

test('a partially written record that fails is ignored', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k1', destination: 'd', payload: {} });
  fs.writeLimit = 5; // force a short, then a failed, write
  fs.injectAfter('write');
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k2', destination: 'd', payload: { v: 2 } }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k1' }), pending(1));
  fs.writeLimit = Infinity;
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k1' }), pending(1));
});

test('a compact whose directory sync takes effect then fails does not shadow later work', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k1', destination: 'd', payload: {} });
  fs.injectAfter('sync');
  assert.deepEqual(engine.handle({ op: 'compact' }), { error: 'io' });
  // Further successful work must survive a power loss, so the uncommitted new
  // generation must have been durably removed.
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k2', destination: 'd', payload: {} }), pending(2));
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k1' }), pending(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k2' }), pending(2));
});

test('a pause whose write takes effect then fails is recovered', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  fs.injectAfter('write');
  assert.deepEqual(engine.handle({ op: 'pause', destination: 'd', until: 10 }), { error: 'io' });
  assert.equal(engine.handle({ op: 'stats' }).destinations.d.paused, true);
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.equal(recovered.handle({ op: 'stats' }).destinations.d.paused, true);
});

test('a compact that fails before creating the new file keeps serving', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k1', destination: 'd', payload: {} });
  fs.inject('write'); // the new generation is never created
  assert.deepEqual(engine.handle({ op: 'compact' }), { error: 'io' });
  assert.deepEqual(engine.handle({ op: 'put', stream: 's', key: 'k2', destination: 'd', payload: {} }), pending(2));
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k1' }), pending(1));
  fs.powerLoss();
  const recovered = new Engine(fs.toFs(), {});
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k1' }), pending(1));
  assert.deepEqual(recovered.handle({ op: 'get', stream: 's', key: 'k2' }), pending(2));
});

test('a step removes a passed pause before selecting work in the same call', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'pause', destination: 'd', until: 5 });
  const out = engine.handle({ op: 'step', now: 5, results: [], budget: 10 });
  assert.deepEqual(out.map((a) => a.stream), ['s']);
});

test('resume of an unpaused destination is a no-op success and leaves it absent', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), {});
  assert.deepEqual(engine.handle({ op: 'resume', destination: 'never-paused' }), { destination: 'never-paused', paused: false });
  assert.equal(Object.prototype.hasOwnProperty.call(engine.handle({ op: 'stats' }).destinations, 'never-paused'), false);
});

test('a second redrive is allowed after the new allowance is exhausted', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 1, reason: null });
  engine.handle({ op: 'step', now: 1, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 2, results: [{ stream: 's', key: 'k', attempt: 2, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'get', stream: 's', key: 'k' }), { seq: 1, state: 'dead', attempts: 2, reason: 'exhausted' });
  assert.deepEqual(engine.handle({ op: 'requeue', stream: 's', key: 'k' }), { seq: 1, state: 'pending', attempts: 2, reason: null });
});

test('stats counts a requeued event as pending', () => {
  const fs = new MockFs();
  const engine = new Engine(fs.toFs(), { leaseMs: 100, baseMs: 4, capMs: 16, maxAttempts: 1 });
  engine.handle({ op: 'put', stream: 's', key: 'k', destination: 'd', payload: {} });
  engine.handle({ op: 'step', now: 0, results: [], budget: 10 });
  engine.handle({ op: 'step', now: 1, results: [{ stream: 's', key: 'k', attempt: 1, code: 500 }], budget: 10 });
  assert.deepEqual(engine.handle({ op: 'stats' }).states, { pending: 0, inflight: 0, acked: 0, dead: 1, cancelled: 0 });
  engine.handle({ op: 'requeue', stream: 's', key: 'k' });
  assert.deepEqual(engine.handle({ op: 'stats' }).states, { pending: 1, inflight: 0, acked: 0, dead: 0, cancelled: 0 });
});
