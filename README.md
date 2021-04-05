# time-series
Read and write call detail records and alerts to an influxdb database.

## Usage
Import a function and invoke it with the ip address of the influxdb database and a [pino](https://www.npmjs.com/package/pino) logger.

```js
const {
  writeCdrs,
  queryCdrs,
  writeAlerts,
  queryAlerts,
  AlertType
} = require('@jambonz/time-series')(pinoLogger, '172.32.10.44');

await writeCdrs([{
    from: 'me',
    to: 'you',
    sip_callid: 'foo@127.0.0.1',
    answered: true,
    duration: 22,
    attempted_at: new Date().getTime() - 40,
    answered_at: new Date().getTime() - 30,
    terminated_at: new Date().getTime(),
    termination_reason: 'caller hungup',
    direction: 'inbound',
    host: '10.10.100.1',
    remote_host: '10.10.100.8',
    trunk: 'device',
    account_sid: 'xxxx'
  }]);

  result = await queryCdrs({account_sid: 'yyyy', trunk: 'device', limit: 10});

  await writeAlerts([{
    alert_type: AlertType.WEBHOOK_FAILURE,
    account_sid: 'yyyy',
    reason: 'your webhook returned 404'
  }]);

  result = await queryAlerts({limit: 10});
```

## cdrs

### tags
- account_sid
- host: ip address of the SBC that received or generated the call
- trunk: name of the carrier that received or generated the call
- direction: 'inbound' or 'outbound'

### fields 
- call_sid
- from
- to
- answered
- sip_callid
- sip_status
- duration
- attempted_at
- answered_at
- terminated_at
- termination_reason
- remote_host

## alerts

### tags
- account_sid
- alert_type

### fields
- url
- vendor
- message

