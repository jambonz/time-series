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
```
### CDRs
#### writing

```
await writeCdrs([{
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
    account_sid: 'xxxx',
    call_sid: 'foo'
  }]);
```
#### reading
Reading is pagination-based: request a page of data, providing the page number and count of records (i.e page size).  

Begin by querying page 1 and a count of records >= 25 and <= 500.  The response will include a "total" property indicating the total number of records meeting the supplied criteria; based on this you can calculate the total number of page and then provide pagination.
```
result = await queryCdrs({account_sid: 'yyyy', page: 1, page_size:50});

{
	"total": 1,
	"page_size": 50,
	"page": 1,
	"data": [{cdr 1}, {cdr 2}, .. {cdr 50}]
}
```

##### filtering data
CDR data has these tags, of which you *must* provide at least `account_sid` in your query, and you may optionally filter the data further by providing any of the other tags and associated values to filter on:
- account_sid
- host (IP address of SBC that generated the CDR)
- trunk (carrier name)
- direction (inbound or outbound)
- answered (true or false)
```
result = await queryCdrs({account_sid: 'yyyy', direction: 'inbound', page: 1, page_size:50});
```
### Alerts
#### writing
```
await writeAlerts([{
      alert_type: AlertType.WEBHOOK_STATUS_FAILURE,
      account_sid: 'yyyy',
      url: 'http://foo.bar',
      status: 404
    },
    {
      alert_type: AlertType.TTS_NOT_PROVISIONED,
      account_sid: 'yyyy',
      vendor: 'google'
    },
    {
      alert_type: AlertType.CARRIER_NOT_PROVISIONED,
      account_sid: 'yyyy',
    },
    {
      alert_type: AlertType.CALL_LIMIT,
      account_sid: 'yyyy',
      count: 50,
    }
]);
```
#### reading
```
result = await queryAlerts({account_sid: 'yyyy', page: 1, page_size:50});

{
	"total": 1,
	"page_size": 50,
	"page": 1,
	"data": [{alert 1}, {alert 2}, .. {alert 50}]
}
```
##### filtering data
Alert data has these tags, of which you *must* provide at least `account_sid` in your query, and you may optionally filter the data further by providing any of the other tags and associated values to filter on:
- account_sid
- alert_type
```
result = await queryAlerts({account_sid: 'yyyy', alert_type: AlertType.CARRIER_NOT_PROVISIONED, page: 1, page_size:50});
```

