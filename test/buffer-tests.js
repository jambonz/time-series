const test = require('tape');
const Influx = require('influx');
const {ServiceNotAvailableError, RequestError} = require('influx/lib/src/pool');

/* These tests cover the write buffer, so they need no influxdb: the client's
 * network calls are replaced for the duration of each test. */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const range = (from, to) => Array.from({length: to - from + 1}, (_, idx) => from + idx);
const sids = (from, to) => range(from, to).map((i) => `call-${i}`);

const cdr = (i) => ({
  from: 'me',
  to: 'you',
  sip_callid: `call-${i}@127.0.0.1`,
  answered: true,
  duration: 22,
  attempted_at: Date.now() - (3600 * 1000) - i,
  terminated_at: Date.now(),
  termination_reason: 'caller hungup',
  direction: 'inbound',
  host: '10.10.100.1',
  remote_host: '10.10.100.8',
  trunk: 'device',
  service_provider_sid: 'zzzzz',
  account_sid: 'xxxx',
  call_sid: `call-${i}`
});
const refused = () => Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:8086'), {code: 'ECONNREFUSED'});

/**
 * Create a time-series instance whose influx client is stubbed:
 * - stub.written: the points of the writes that succeeded, in order
 *   (a cdr by its call_sid, a call count as calls-<calls_in_progress>)
 * - stub.writes: the number of points in each write attempted
 * - stub.failures: errors to fail the next writes with
 * - stub.errors: the arguments of each logger.error call
 * - tick(): run the periodic flush now, instead of waiting for its interval
 */
const setup = (t, opts) => {
  const stub = {written: [], writes: [], failures: [], errors: [], delay: 20};
  const saved = {
    writePoints: Influx.InfluxDB.prototype.writePoints,
    getDatabaseNames: Influx.InfluxDB.prototype.getDatabaseNames,
    setInterval: global.setInterval
  };
  Influx.InfluxDB.prototype.getDatabaseNames = async() => ['cdrs', 'call_counts'];
  Influx.InfluxDB.prototype.writePoints = async function(points) {
    /* like the real client, take the points as they are when the write starts */
    const labels = points.map((p) => p.fields.call_sid || `calls-${p.fields.calls_in_progress}`);
    stub.writes.push(labels.length);
    const err = stub.failures.shift();
    await sleep(stub.delay);
    if (err) throw err;
    stub.written.push(...labels);
  };
  const ticks = [];
  global.setInterval = (fn) => ticks.push(fn);
  const logger = {error: (...args) => stub.errors.push(args), info: () => {}, debug: () => {}};
  const fns = require('..')(logger, {host: '127.0.0.1', ...opts});
  global.setInterval = saved.setInterval;
  t.teardown(() => {
    Influx.InfluxDB.prototype.writePoints = saved.writePoints;
    Influx.InfluxDB.prototype.getDatabaseNames = saved.getDatabaseNames;
  });
  const tick = async() => {
    ticks.forEach((fn) => fn());
    await sleep(stub.delay * 3);
  };
  return {...fns, stub, tick};
};

test('cdrs queued while a write is in flight are not lost (default options)', async(t) => {
  const {writeCdrs, stub} = setup(t, {});

  await Promise.all(range(1, 3).map((i) => writeCdrs(cdr(i))));
  await sleep(stub.delay * 3);

  t.deepEqual(stub.written, sids(1, 3), 'all three overlapping cdrs were written');
  t.deepEqual(stub.writes, [1, 2], 'the two queued during the first write went out together right after it');
});

test('cdrs queued while a write is in flight are not lost (commitSize 50)', async(t) => {
  const {writeCdrs, stub, tick} = setup(t, {commitSize: 50, commitInterval: 20});

  /* 80 calls end at the same moment: the 50th fills a batch, the other 30 are queued during its write */
  await Promise.all(range(1, 80).map((i) => writeCdrs(cdr(i))));
  t.deepEqual(stub.written, sids(1, 50), 'the first 50 cdrs were written when commitSize was reached');
  await tick();
  t.deepEqual(stub.written, sids(1, 80), 'the 30 queued during that write were written by the next periodic flush');

  /* 200 at once: enough are queued during the first write to fill more batches */
  stub.written.length = 0;
  await Promise.all(range(101, 300).map((i) => writeCdrs(cdr(i))));
  t.deepEqual(stub.written, sids(101, 300), 'all 200 cdrs of the burst were written without waiting for the timer');
});

test('cdrs are kept for the next flush when influxdb is unavailable', async(t) => {
  const {writeCdrs, stub, tick} = setup(t, {commitSize: 50, commitInterval: 20});

  stub.failures.push(refused(), new ServiceNotAvailableError('Service Unavailable'));
  const results = await Promise.allSettled(range(1, 50).map((i) => writeCdrs(cdr(i))));
  t.equal(results.filter((r) => 'rejected' === r.status).length, 1,
    'the call that triggered the failed write is rejected');
  t.deepEqual(stub.written, [], 'nothing written yet');

  await tick();
  t.deepEqual(stub.writes, [50, 50], 'the periodic flush tried again');
  t.deepEqual(stub.written, [], 'and failed again (503)');

  await Promise.all(range(51, 99).map((i) => writeCdrs(cdr(i))));
  t.deepEqual(stub.writes, [50, 50], 'kept cdrs do not count towards commitSize: no extra attempt meanwhile');

  await tick();
  t.deepEqual(stub.written, sids(1, 99), 'every cdr was written, in order, once influxdb was back');
  t.deepEqual(stub.writes, [50, 50, 99], 'the kept cdrs went out with those queued meanwhile');
  t.equal(stub.errors.length, 2, 'each failed write was logged');
  t.ok(/kept 50 for the next flush, dropped 0/.test(stub.errors[0][1]), 'the log says what was kept');
});

test('a batch that influxdb rejects is dropped, not retried', async(t) => {
  const {writeCdrs, stub, tick} = setup(t, {commitSize: 50, commitInterval: 20});

  const badRequest = {statusCode: 400, statusMessage: 'Bad Request'};
  stub.failures.push(new RequestError({}, badRequest, '{"error":"partial write"}'));
  const results = await Promise.allSettled(range(1, 50).map((i) => writeCdrs(cdr(i))));
  t.equal(results.filter((r) => 'rejected' === r.status).length, 1, 'the call that triggered the write is rejected');

  await Promise.all(range(51, 100).map((i) => writeCdrs(cdr(i))));
  await tick();
  t.deepEqual(stub.written, sids(51, 100), 'later cdrs are written and the rejected batch is not sent again');
  t.ok(/kept 0 for the next flush, dropped 50/.test(stub.errors[0][1]), 'the dropped batch was logged');
});

test('points without a timestamp are not kept for the next flush', async(t) => {
  const {writeCallCount, stub, tick} = setup(t, {commitSize: 2, commitInterval: 20});

  stub.failures.push(refused());
  const results = await Promise.allSettled([1, 2].map((n) =>
    writeCallCount({calls_in_progress: n, service_provider_sid: 'zzzzz', account_sid: 'xxxx'})));
  t.equal(results.filter((r) => 'rejected' === r.status).length, 1, 'the write of the two call counts failed');

  await tick();
  t.deepEqual(stub.writes, [2], 'they were not tried again');
  t.ok(/kept 0 for the next flush, dropped 2/.test(stub.errors[0][1]), 'dropping them was logged');
});

test('the points kept for the next flush are bounded', async(t) => {
  const {writeCdrs, stub, tick} = setup(t, {commitSize: 50, commitInterval: 20, maxRetryPoints: 100});

  /* influxdb is down for five writes in a row */
  for (const from of [1, 51, 101, 151, 201]) {
    stub.failures.push(refused());
    await Promise.allSettled(range(from, from + 49).map((i) => writeCdrs(cdr(i))));
  }
  await tick();

  t.deepEqual(stub.written, sids(151, 250), 'only the newest maxRetryPoints cdrs were kept');
  t.ok(stub.errors.some((args) => /kept 100 for the next flush, dropped 50/.test(args[1])),
    'dropping the oldest was logged');
});

test('a failed periodic flush does not leave an unhandled rejection', async(t) => {
  const {writeCdrs, stub, tick} = setup(t, {commitSize: 50, commitInterval: 20});
  const unhandled = [];
  const onUnhandled = (err) => unhandled.push(err);
  process.on('unhandledRejection', onUnhandled);
  t.teardown(() => process.removeListener('unhandledRejection', onUnhandled));

  await Promise.all(range(1, 5).map((i) => writeCdrs(cdr(i))));
  stub.failures.push(refused());
  await tick();
  await sleep(50);
  t.equal(unhandled.length, 0, 'no unhandled rejection');
  t.deepEqual(stub.written, [], 'nothing was written while influxdb was down');

  await tick();
  t.deepEqual(stub.written, sids(1, 5), 'the cdrs were written by the next periodic flush');
});
