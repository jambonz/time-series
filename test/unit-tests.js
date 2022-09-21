const test = require('tape');
const Influx = require('influx');
const consoleLogger = {error: console.error, info: console.log, debug: console.log};

const {
  writeCallCount,
  writeCallCountSP,
  writeCallCountApp,
  queryCallCounts,
  queryCallCountsSP,
  queryCallCountsApp,
  writeCdrs,
  queryCdrs,
  queryCdrsSP,
  writeAlerts,
  queryAlerts,
  queryAlertsSP,
  AlertType
} = require('..')(consoleLogger, '127.0.0.1', {commitSize: 1});

test('write timeseries data', async(t) => {
  let result = await writeCdrs([{
    from: 'me',
    to: 'you',
    sip_callid: 'foo@127.0.0.1',
    answered: true,
    duration: 22,
    attempted_at: new Date(Date.now() - (3600 * 1000)),
    answered_at: Date.now() - (3590 * 1000),
    terminated_at: Date.now(),
    termination_reason: 'caller hungup',
    direction: 'inbound',
    host: '10.10.100.1',
    remote_host: '10.10.100.8',
    trunk: 'device',
    service_provider_sid: 'zzzzz',
    account_sid: 'xxxx',
  },
  {
    from: 'me2',
    to: 'you2',
    sip_callid: 'foo@127.0.0.1',
    answered: false,
    duration: 20,
    attempted_at: new Date(Date.now() - (7200 * 1000)),
    answered_at: Date.now() - (7180 * 1000),
    terminated_at: Date.now(),
    termination_reason: 'caller hungup',
    direction: 'inbound',
    host: '10.10.100.1',
    remote_host: '10.10.100.8',
    trunk: 'twilio',
    service_provider_sid: 'zzzzz',
    account_sid: 'yyyy',
    call_sid: 'bar',
    application_sid: 'app-sid'
  }]);
  t.pass('wrote cdr');

  result = await queryCdrs({account_sid: 'xxxx', page: 1, page_size:25});
  //console.log(JSON.stringify(result));
  t.ok(result.data.length === 1, 'queried cdrs by account sid')

  result = await queryCdrs({account_sid: 'yyyy', trunk: 'twilio', page: 1, page_size:25});
  t.ok(result.data.length === 1, 'queried cdrs by trunk')

  result = await queryCdrsSP({service_provider_sid: 'zzzzz', page: 1, page_size:25});
  t.ok(result.data.length === 2, 'queried cdrs by service provider sid')

  result = await writeAlerts([
    {
      alert_type: AlertType.WEBHOOK_STATUS_FAILURE,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      url: 'http://foo.bar',
      status: 404
    },
    {
      alert_type: AlertType.WEBHOOK_CONNECTION_FAILURE,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      url: 'http://foo.bar'
    },
    {
      alert_type: AlertType.WEBHOOK_AUTH_FAILURE,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      url: 'http://foo.bar'
    },
    {
      alert_type: AlertType.INVALID_APP_PAYLOAD,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      target_sid: 'zzzz',
      message: 'invalid app payload'
    },
    {
      alert_type: AlertType.TTS_NOT_PROVISIONED,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      vendor: 'google'
    },
    {
      alert_type: AlertType.STT_NOT_PROVISIONED,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      vendor: 'google'
    },
    {
      alert_type: AlertType.TTS_FAILURE,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      vendor: 'google'
    },
    {
      alert_type: AlertType.STT_FAILURE,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      vendor: 'google'
    },
    {
      alert_type: AlertType.CARRIER_NOT_PROVISIONED,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
    },
    {
      alert_type: AlertType.SP_CALL_LIMIT,
      service_provider_sid: 'zzzzz',
      count: 50,
    },
    {
      alert_type: AlertType.ACCOUNT_CALL_LIMIT,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      count: 50,
    },
    {
      alert_type: AlertType.SP_DEVICE_LIMIT,
      service_provider_sid: 'zzzzz',
      count: 500,
    },
    {
      alert_type: AlertType.ACCOUNT_DEVICE_LIMIT,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      count: 250,
    },
    {
      alert_type: AlertType.ACCOUNT_API_LIMIT,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy',
      count: 120,
    },
    {
      alert_type: AlertType.SP_API_LIMIT,
      service_provider_sid: 'zzzzz',
      count: 300,
    }
  ]);
  t.pass('wrote alerts');

  result = await queryAlerts({account_sid: 'yyyy', page: 1, page_size: 25, days: 7});
  //console.log(JSON.stringify(result));
  t.ok(result.data.length === 12, 'queried alerts');
  t.ok(result.data[0].target_sid === null)

  // Make sure that page, page_size and days support both string and int
  result = await queryAlerts({account_sid: 'yyyy', page: '1', page_size: '10', days: '7'});
  t.ok(result.data.length === 10, 'queried alerts');

  result = await queryAlerts({account_sid: 'yyyy', page: '2', page_size: '10', days: '7'});
  t.ok(result.data.length === 2, 'queried alerts');

  result = await queryAlerts({account_sid: 'yyyy', target_sid: 'zzzz', page: 1, page_size: 25, days: 7});
  t.ok(result.data.length === 1, 'queried alerts by target_sid');
  t.ok(result.data[0].target_sid === 'zzzz')

  result = await queryAlertsSP({service_provider_sid: 'zzzzz', page: 1, page_size: 25, days: 7});
  //console.log(result);
  t.ok(result.data.length === 15, 'queried alerts by service_provider_sid');

  result = await writeCallCount(
    {
      calls_in_progress: 49,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy'
    });
  result = await writeCallCount(
    {
      calls_in_progress: 50,
      service_provider_sid: 'zzzzz',
      account_sid: 'yyyy'
    });
  t.pass('wrote call counts for account');

  result = await writeCallCountSP(
    {
      calls_in_progress: 500,
      service_provider_sid: 'zzzzz'
    });
  result = await writeCallCountSP(
    {
      calls_in_progress: 501,
      service_provider_sid: 'zzzzz'
    });
  t.pass('wrote call counts for service provider');

  result = await writeCallCountApp(
    {
      calls_in_progress: 20,
      service_provider_sid: 'zzzzz',
      application_sid: 'zzzzz'
    });
  result = await writeCallCountApp(
    {
      calls_in_progress: 21,
      service_provider_sid: 'zzzzz',
      account_sid: 'zzzzz',
      application_sid: 'zzzzz'
    });
  t.pass('wrote call counts for application');

  result = await queryCallCountsSP({service_provider_sid: 'zzzzz', page: 1, page_size: 25, days: 7});
  //console.log(JSON.stringify(result));
  t.ok(result.data.length === 2, 'queried call counts by service provider sid');

  result = await queryCallCounts({account_sid: 'yyyy', page: 1, page_size: 25, days: 7});
  //console.log(JSON.stringify(result));
  t.ok(result.data.length === 2, 'queried call counts by account_sid');

  result = await queryCallCountsApp({application_sid: 'zzzzz', page: 1, page_size: 25, days: 7});
  //console.log(JSON.stringify(result));
  t.ok(result.data.length === 2 && result.data[0].calls_in_progress === 21, 'queried call counts by application_sid');

});
