const assert = require('assert');
const Influx = require('influx');
const AlertType = {
  WEBHOOK_STATUS_FAILURE: 'webhook-failure',
  WEBHOOK_CONNECTION_FAILURE: 'webhook-connection-failure',
  WEBHOOK_URL_NOTFOUND: 'webhook-url-notfound',
  WEBHOOK_AUTH_FAILURE: 'webhook-auth-failure',
  INVALID_APP_PAYLOAD: 'invalid-app-payload',
  TTS_NOT_PROVISIONED: 'no-tts',
  STT_NOT_PROVISIONED: 'no-stt',
  TTS_FAILURE: 'tts-failure',
  STT_FAILURE: 'stt-failure',
  CARRIER_NOT_PROVISIONED: 'no-carrier',
  CALL_LIMIT: 'call-limit',
  DEVICE_LIMIT: 'device-limit',
  API_LIMIT: 'api-limit',
  ACCOUNT_INACTIVE: 'account is inactive or suspended'
};

const schemas = {
  cdrs: {
    measurement: 'cdrs',
    fields: {
      call_sid: Influx.FieldType.STRING,
      from: Influx.FieldType.STRING,
      to: Influx.FieldType.STRING,
      sip_callid: Influx.FieldType.STRING,
      sip_status: Influx.FieldType.INTEGER,
      duration: Influx.FieldType.INTEGER,
      terminated_at: Influx.FieldType.INTEGER,
      termination_reason: Influx.FieldType.STRING,
      remote_host: Influx.FieldType.STRING,
      trace_id: Influx.FieldType.STRING
    },
    tags: [
      'account_sid',
      'host',
      'trunk',
      'direction',
      'answered'
    ]
  },
  alerts: {
    measurement: 'alerts',
    fields: {
      message: Influx.FieldType.STRING,
      detail: Influx.FieldType.STRING,
    },
    tags: [
      'account_sid',
      'alert_type'
    ]
  },
  call_counts: {
    measurement: 'call_counts',
    fields: {
      calls_in_progress: Influx.FieldType.INTEGER,
    },
    tags: [
      'account_sid'
    ]
  }
};

const writeData = async(client) => {
  const {data, initialized} = client.locals;
  if (0 === data.length || client.locals.writing) return;
  if (!initialized) await initDatabase(client, client.locals.db);
  try {
    //console.log(`writing data ${JSON.stringify(data)}`);
    client.locals.writing = true;
    await client.writePoints(data);
    client.locals.writing = false;
    client.locals.data = [];
  } catch (err) {
    console.log(err);
    client.locals.writing = false;
    client.locals.data = [];
    throw err;
  }
};

const createCallCountsQuery = ({account_sid, page, page_size, days, start, end}) => {
  let sql = `SELECT * from call_counts WHERE account_sid = '${account_sid}'`;
  if (days) sql += `AND time > now() - ${days}d `;
  else {
    if (start) sql += `AND time >= '${start}' `;
    if (end) sql += `AND time <= '${end}' `;
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ` LIMIT ${page_size}`;
  if (page) sql += ` OFFSET ${(page - 1) * page_size}`;
  //console.log(sql);
  return sql;
};

const createCallCountsCountQuery = ({account_sid, days, start, end}) => {
  let sql = `SELECT COUNT(calls_in_progress) from call_counts WHERE account_sid = '${account_sid}' `;
  if (days) sql += `AND time > now() - ${days}d `;
  else {
    if (start) sql += `AND time >= '${start}' `;
    if (end) sql += `AND time <= '${end}' `;
  }
  //console.log(sql);
  return sql;
};

const createCdrQuery = ({account_sid, page, page_size, trunk, direction, answered, days, start, end}) => {
  let sql = `SELECT * from cdrs WHERE account_sid = '${account_sid}'`;
  if (trunk) sql += `AND trunk = '${trunk}' `;
  if (direction) sql += `AND direction = '${direction}' `;
  if (['true', 'false'].includes(answered)) sql += `AND answered = '${answered}' `;
  if (days) sql += `AND time > now() - ${days}d `;
  else {
    if (start) sql += `AND time >= '${start}' `;
    if (end) sql += `AND time <= '${end}' `;
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ` LIMIT ${page_size}`;
  if (page) sql += ` OFFSET ${(page - 1) * page_size}`;
  //console.log(sql);
  return sql;
};
const createCdrCountQuery = ({account_sid, trunk, direction, answered, days, start, end}) => {
  let sql = `SELECT COUNT(sip_callid) from cdrs WHERE account_sid = '${account_sid}' `;
  if (trunk) sql += `AND trunk = '${trunk}' `;
  if (direction) sql += `AND direction = '${direction}' `;
  if (['true', 'false'].includes(answered)) sql += `AND answered = '${answered}' `;
  if (days) sql += `AND time > now() - ${days}d `;
  else {
    if (start) sql += `AND time >= '${start}' `;
    if (end) sql += `AND time <= '${end}' `;
  }
  //console.log(sql);
  return sql;
};

const createAlertsQuery = ({account_sid, target_sid, alert_type, page, page_size, days, start, end}) => {
  let sql = `SELECT * FROM alerts WHERE account_sid = '${account_sid}' `;
  if (target_sid) sql += `AND target_sid = '${target_sid}' `;
  if (alert_type) sql += `AND alert_type = '${alert_type}' `;
  if (days) sql += `AND time > now() - ${days}d `;
  else {
    if (start) sql += `AND time >= '${start}' `;
    if (end) sql += `AND time <= '${end}' `;
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ` LIMIT ${page_size}`;
  if (page) sql += ` OFFSET ${(page - 1) * page_size}`;
  //console.log(sql);
  return sql;
};

const createAlertsCountQuery = ({account_sid, target_sid, alert_type, days, start, end}) => {
  let sql = `SELECT COUNT(message) FROM alerts WHERE account_sid = '${account_sid}' `;
  if (target_sid) sql += `AND target_sid = '${target_sid}' `;
  if (alert_type) sql += `AND alert_type = '${alert_type}' `;
  if (days) sql += `AND time > now() - ${days}d `;
  else {
    if (start) sql += `AND time >= '${start}' `;
    if (end) sql += `AND time <= '${end}' `;
  }
  //console.log(sql);
  return sql;
};

const initDatabase = async(client, dbName) => {
  const names = await client.getDatabaseNames();
  if (!names.includes(dbName)) {
    await client.createDatabase(dbName);
  }
  client.locals.initialized = true;
};

const writeCallCount = async(client, count) => {
  if (!client.locals.initialized) await initDatabase(client, 'call_counts');
  const {account_sid, ...fields} = count;
  const data = {
    measurement: 'call_counts',
    fields,
    tags: {
      account_sid
    }
  };
  client.locals.data = [...client.locals.data, ...[data]];
  if (client.locals.data.length >= client.locals.commitSize) {
    await writeData(client);
  }
  return;
};

const queryCallCounts = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'call_counts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const sqlTotal = createCallCountsCountQuery(opts);
  const obj = await client.queryRaw(sqlTotal);
  //console.log(`sqlTotal: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCallCountsQuery(opts);
  const res = await client.queryRaw(sql);
  //console.log(`sql: ${sqlTotal}, results: ${JSON.stringify(res)}`);
  if (res.results[0].series && res.results[0].series.length) {
    const {columns, values} = res.results[0].series[0];
    const data = values.map((v) => {
      const obj = {};
      v.forEach((val, idx) => {
        v.forEach((val, idx) => {
          const key = columns[idx];
          obj[key] = val;
        });
      });
      return obj;
    });
    response.data = data;
  }
  return response;
};

const writeCdrs = async(client, cdrs) => {
  if (!client.locals.initialized) await initDatabase(client, 'cdrs');
  cdrs = (Array.isArray(cdrs) ? cdrs : [cdrs])
    .map((cdr) => {
      const {direction, host, trunk, account_sid, answered, attempted_at, ...fields} = cdr;
      return {
        measurement: 'cdrs',
        timestamp: new Date(attempted_at),
        fields,
        tags: {
          direction,
          host,
          trunk,
          account_sid,
          answered
        }
      };
    });
  //console.log(`writing cdrs: ${JSON.stringify(cdrs)}`);
  client.locals.data = [...client.locals.data, ...cdrs];
  if (client.locals.data.length >= client.locals.commitSize) {
    await writeData(client);
  }
  return;
};

const queryCdrs = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'cdrs');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const sqlTotal = createCdrCountQuery(opts);
  const obj = await client.queryRaw(sqlTotal);
  //console.log(`sql: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCdrQuery(opts);
  const res = await client.queryRaw(sql);
  if (res.results[0].series && res.results[0].series.length) {
    const {columns, values} = res.results[0].series[0];
    const data = values.map((v) => {
      const obj = {};
      v.forEach((val, idx) => {
        const key = 'time' === columns[idx] ? 'attempted_at' : columns[idx];
        let retvalue = val;
        if (['answered_at', 'terminated_at'].includes(key)) retvalue = new Date(val);
        if (key === 'answered') retvalue = 'true' === val ? true : false;
        obj[key] = retvalue;
      });
      return obj;
    });
    response.data = data;
  }
  return response;
};

const writeAlerts = async(client, alerts) => {
  if (!client.locals.initialized) await initDatabase(client, 'alerts');
  alerts = (Array.isArray(alerts) ? alerts : [alerts])
    .map((alert) => {
      const {alert_type, account_sid, target_sid, url, status, vendor, count, detail, timestamp} = alert;
      let message = alert.message;
      if (!message) {
        switch (alert_type) {
          case AlertType.WEBHOOK_STATUS_FAILURE:
            message = `${url} returned ${status}`;
            break;
          case AlertType.WEBHOOK_CONNECTION_FAILURE:
            message = `failed to connect to ${url}`;
            break;
          case AlertType.WEBHOOK_AUTH_FAILURE:
            message = `authentication failure: ${url}`;
            break;
          case AlertType.WEBHOOK_URL_NOTFOUND:
            message = `webhook url not found: ${url}`;
            break;
          case AlertType.INVALID_APP_PAYLOAD:
            message = `${url} return invalid app payload`;
            break;
          case AlertType.TTS_NOT_PROVISIONED:
            message = `text to speech credentials for ${vendor} have not been provisioned`;
            break;
          case AlertType.STT_NOT_PROVISIONED:
            message = `speech to text credentials for ${vendor} have not been provisioned`;
            break;
          case AlertType.TTS_FAILURE:
            message = `text to speech request to ${vendor} failed; please check your speech credentials`;
            break;
          case AlertType.STT_FAILURE:
            message = `speech to text request to ${vendor} failed; please check your speech credentials`;
            break;
          case AlertType.CARRIER_NOT_PROVISIONED:
            message = 'outbound call failure: no carriers have been provisioned';
            break;
          case AlertType.CALL_LIMIT:
            message = `you have exceeded your provisioned call limit of ${count}; please consider upgrading your plan`;
            break;
          case AlertType.DEVICE_LIMIT:
            message =
              `you have exceeded your device registration limit of ${count}; please consider upgrading your plan`;
            break;
          case AlertType.API_LIMIT:
            message = `you have exceeded your api limit of ${count}; please consider upgrading your plan`;
            break;
          default:
            break;
        }
      }
      let tags =  { alert_type, account_sid };
      if (target_sid) Object.assign(tags, {target_sid});
      const obj = {measurement: 'alerts', fields: { message }, tags: tags};
      if (timestamp) obj.timestamp = timestamp;
      if (detail) obj.fields.detail = detail;
      return obj;
    });
  //console.log(`writing alerts: ${JSON.stringify(alerts)}`);
  client.locals.data = [...client.locals.data, ...alerts];
  if (client.locals.data.length >= client.locals.commitSize) {
    await writeData(client);
  }
  return;
};

const queryAlerts = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'alerts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const sqlTotal = createAlertsCountQuery(opts);
  const obj = await client.queryRaw(sqlTotal);
  //console.log(`query total alerts: ${sqlTotal}: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createAlertsQuery(opts);
  const res = await client.queryRaw(sql);
  if (res.results[0].series && res.results[0].series.length) {
    const {columns, values} = res.results[0].series[0];
    const data = values.map((v) => {
      const obj = {};
      v.forEach((val, idx) => {
        const key = columns[idx];
        obj[key] = val;
      });
      return obj;
    });
    response.data = data;
  }
  return response;
};

module.exports = (logger, opts) => {
  if (typeof opts === 'string') opts = {host: opts};
  assert(opts.host);

  const cdrClient = new Influx.InfluxDB({database: 'cdrs', schemas: schemas.cdr, ...opts});
  const alertClient = new Influx.InfluxDB({database: 'alerts', schemas: schemas.alerts, ...opts});
  const callCountClient = new Influx.InfluxDB({database: 'call_counts', schemas: schemas.call_counts, ...opts});

  cdrClient.locals = {
    db: 'cdrs',
    initialized: false,
    writing: false,
    commitSize: opts.commitSize || 1,
    commitInterval: opts.commitInterval || 10,
    data: []
  };
  alertClient.locals = {
    db: 'alerts',
    initialized: false,
    writing: false,
    commitSize: opts.commitSize || 1,
    commitInterval: opts.commitInterval || 10,
    data: []
  };
  callCountClient.locals = {
    db: 'call_counts',
    initialized: false,
    writing: false,
    commitSize: opts.commitSize || 1,
    commitInterval: opts.commitInterval || 10,
    data: []
  };

  if (opts.commitSize > 1 && opts.commitInterval && opts.commitInterval > 2) {
    setInterval(writeData.bind(null, callCountClient), opts.commitInterval * 1000);
    setInterval(writeData.bind(null, cdrClient), opts.commitInterval * 1000);
    setInterval(writeData.bind(null, alertClient), opts.commitInterval * 1000);
  }

  return {
    writeCallCount: writeCallCount.bind(null, callCountClient),
    queryCallCounts: queryCallCounts.bind(null, callCountClient),
    writeCdrs: writeCdrs.bind(null, cdrClient),
    queryCdrs: queryCdrs.bind(null, cdrClient),
    writeAlerts: writeAlerts.bind(null, alertClient),
    queryAlerts: queryAlerts.bind(null, alertClient),
    AlertType: { ...AlertType }
  };
};
