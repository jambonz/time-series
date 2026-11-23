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
### Options
Instead of the address, you can pass an object of options. It is also handed to the [influx](https://github.com/node-influx/node-influx) client, so the client's connection options (`port`, `username`, `password`, ...) work too.

```js
const {writeCdrs} = require('@jambonz/time-series')(pinoLogger, {
  host: '172.32.10.44',
  commitSize: 50,
  commitInterval: 20
});
```
- `host`: the address of the influxdb database (required).
- `commitSize`: how many points to buffer for a database before writing them (default 1: every write call writes straight away). `writeRatedCdrs`, `writeCallMargin` and `writeSystemAlerts` always write straight away, taking the points buffered for their database with them.
- `commitInterval`: when `commitSize` is more than 1, how often to write the buffered points, in seconds. A value of 2 or less turns this off. Without it, buffered points wait for a write call that fills the buffer or always writes.
- `maxRetryPoints`: after failed writes, how many points at most to keep for each database, to write again with the next write (default 10000). Beyond that, the oldest are dropped.
  - Points are kept when influxdb could not take them (a connection error, a timeout or a 5xx), and only those with their own timestamp, such as call detail records.
  - The points of `writeRatedCdrs` are never kept.
  - After any other failure, such as a 4xx, the points are not written again.

A write call resolves once the write that holds its points succeeds, and is rejected if that write fails.
- While a write is in flight, a call that writes waits for it, then for its own write.
- If the write ahead failed because influxdb could not take it, the waiting calls are rejected with its error and their write is not tried. Their points stay in the buffer for the next write, except those of `writeRatedCdrs`, which are dropped.

A rejected call's points can still end up stored:
- they may be written again with the next write;
- influxdb may have stored them anyway: a write that timed out may still be applied, and a 400 "partial write" stores the valid points of the batch.

With `commitSize` above 1, a call whose points do not fill the buffer resolves straight away, and its points go out with a later write.

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

