const test = require('tape');
const Influx = require('influx');

/* These tests cover how the query functions read influxdb's answer, so they
 * need no influxdb: the client's queryRaw is replaced for each test.
 * influxdb 1.x reports a failed statement with http 200 and an error on that
 * statement's result. */
const failed = {results: [{statement_id: 0, error: 'max-select-series limit exceeded: (2/1)'}]};
const count = (n) => ({results: [{statement_id: 0, series: [{name: 'cdrs', columns: ['time', 'count'],
  values: [['1970-01-01T00:00:00Z', n]]}]}]});

const setup = (t, answers) => {
  const saved = {
    queryRaw: Influx.InfluxDB.prototype.queryRaw,
    getDatabaseNames: Influx.InfluxDB.prototype.getDatabaseNames
  };
  Influx.InfluxDB.prototype.getDatabaseNames = async() =>
    ['cdrs', 'alerts', 'call_counts', 'sp_call_counts', 'app_call_counts', 'llm_usage'];
  Influx.InfluxDB.prototype.queryRaw = async() => answers.shift();
  t.teardown(() => Object.assign(Influx.InfluxDB.prototype, saved));
  const logger = {error: () => {}, info: () => {}, debug: () => {}};
  return require('..')(logger, {host: '127.0.0.1'});
};

const opts = {account_sid: 'xxxx', service_provider_sid: 'zzzzz', application_sid: 'yyyy', page: 1, page_size: 25};
const window = {trunk: 'carrier', start: Date.now() - 3600000, end: Date.now()};
const cases = {
  queryCdrs: opts,
  queryCdrsSP: opts,
  queryAlerts: opts,
  queryAlertsSP: opts,
  queryCallCounts: opts,
  queryCallCountsSP: opts,
  queryCallCountsApp: opts,
  queryAlertsSince: {sinceNs: 0, untilNs: Date.now() * 1000000},
  queryRatingCandidates: window,
  queryDailyUsageTotals: window,
  queryLlmUsage: {startISO: new Date(window.start).toISOString(), endISO: new Date(window.end).toISOString()}
};

for (const [name, args] of Object.entries(cases)) {
  test(`${name} rejects when influxdb reports a statement error`, async(t) => {
    const fns = setup(t, [failed]);
    try {
      const res = await fns[name](args);
      t.fail(`resolved with ${JSON.stringify(res)}`);
    } catch (err) {
      t.ok(/influxdb query failed: max-select-series limit exceeded/.test(err.message), 'rejected with the error');
    }
  });
}

test('queryCdrs rejects when the count succeeds and the page query fails', async(t) => {
  const {queryCdrs} = setup(t, [count(2), failed]);
  try {
    const res = await queryCdrs(opts);
    t.fail(`resolved with ${JSON.stringify(res)}`);
  } catch (err) {
    t.ok(/influxdb query failed/.test(err.message), 'rejected instead of returning the total with no data');
  }
});

test('an empty result is still an empty result', async(t) => {
  const {queryCdrs, queryRatingCandidates} = setup(t, [{results: [{statement_id: 0}]}, {results: [{statement_id: 0}]}]);
  t.deepEqual(await queryCdrs(opts), {total: 0, page_size: 25, page: 1, data: []}, 'queryCdrs');
  t.deepEqual(await queryRatingCandidates(window), [], 'queryRatingCandidates');
});
