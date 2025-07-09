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
  ACCOUNT_CALL_LIMIT: 'account-call-limit',
  ACCOUNT_DEVICE_LIMIT: 'account-device-limit',
  ACCOUNT_API_LIMIT: 'account-api-limit',
  SP_CALL_LIMIT: 'service-provider-call-limit',
  SP_DEVICE_LIMIT: 'service-provider-device-limit',
  SP_API_LIMIT: 'service-provider-api-limit',
  ACCOUNT_INACTIVE: 'account is inactive or suspended',
  PLAY_FILENOTFOUND: 'play-url-notfound',
  TTS_STREAMING_CONNECTION_FAILURE: 'tts-streaming-connection-failure',
  APPLICATION: 'alert-from-application'
};

const schemas = {
  cdrs: {
    measurement: 'cdrs',
    fields: {
      call_sid: Influx.FieldType.STRING,
      application_sid: Influx.FieldType.STRING,
      from: Influx.FieldType.STRING,
      to: Influx.FieldType.STRING,
      sip_callid: Influx.FieldType.STRING,
      sip_parent_callid: Influx.FieldType.STRING,
      sip_status: Influx.FieldType.INTEGER,
      duration: Influx.FieldType.INTEGER,
      terminated_at: Influx.FieldType.INTEGER,
      termination_reason: Influx.FieldType.STRING,
      remote_host: Influx.FieldType.STRING,
      trace_id: Influx.FieldType.STRING,
      recording_url: Influx.FieldType.STRING
    },
    tags: [
      'service_provider_sid',
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
      'service_provider_sid',
      'account_sid',
      'alert_type',
      'vendor'
    ]
  },
  call_counts: {
    measurement: 'call_counts',
    fields: {
      calls_in_progress: Influx.FieldType.INTEGER,
    },
    tags: [
      'service_provider_sid',
      'account_sid'
    ]
  },
  sp_call_counts: {
    measurement: 'sp_call_counts',
    fields: {
      calls_in_progress: Influx.FieldType.INTEGER,
    },
    tags: [
      'service_provider_sid'
    ]
  },
  app_call_counts: {
    measurement: 'app_call_counts',
    fields: {
      calls_in_progress: Influx.FieldType.INTEGER,
    },
    tags: [
      'service_provider_sid',
      'account_sid',
      'application_sid'
    ]
  },
  system_alerts: {
    measurement: 'system_alerts',
    fields: {
      detail: Influx.FieldType.STRING,
      host: Influx.FieldType.STRING
    },
    tags: [
      'system_component',
      'state'
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

/* for application */
const createCallCountsQueryApp = ({page, page_size, days, start, end}) => {
  let sql = 'SELECT * from app_call_counts WHERE application_sid = $application_sid ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};

const createCallCountsCountQueryApp = ({days, start, end}) => {
  let sql = 'SELECT COUNT(calls_in_progress) from app_call_counts WHERE application_sid = $application_sid ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  return sql;
};

/* for Service Provider */
const createCallCountsQuerySP = ({page, page_size, days, start, end}) => {
  let sql = 'SELECT * from sp_call_counts WHERE service_provider_sid = $service_provider_sid ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};

const createCallCountsCountQuerySP = ({days, start, end}) => {
  let sql = 'SELECT COUNT(calls_in_progress) from sp_call_counts WHERE service_provider_sid = $service_provider_sid ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  return sql;
};

/* for Account */
const createCallCountsQuery = ({page, page_size, days, start, end}) => {
  let sql = 'SELECT * from call_counts WHERE account_sid = $account_sid ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};

const createCallCountsCountQuery = ({days, start, end}) => {
  let sql = 'SELECT COUNT(calls_in_progress) from call_counts WHERE account_sid = $account_sid ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  return sql;
};

/* for Service Provider */
const createCdrQuerySP = ({page, page_size, trunk, direction, answered, filter, days, start, end}) => {
  let sql = 'SELECT * from cdrs WHERE service_provider_sid = $service_provider_sid ';
  if (trunk) sql += 'AND trunk = $trunk ';
  if (direction) sql += 'AND direction = $direction ';
  if (['true', 'false'].includes(answered)) sql += 'AND answered = $answered ';
  if (filter) sql += `AND ("from" =~ /.*${filter}.*/ OR "to" =~ /.*${filter}.*/ OR call_sid = $filter) `;
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};
const createCdrCountQuerySP = ({trunk, direction, answered, filter, days, start, end}) => {
  let sql = 'SELECT COUNT(sip_callid) from cdrs WHERE service_provider_sid = $service_provider_sid ';
  if (trunk) sql += 'AND trunk = $trunk ';
  if (direction) sql += 'AND direction = $direction ';
  if (['true', 'false'].includes(answered)) sql += 'AND answered = $answered ';
  if (filter) sql += `AND ("from" =~ /.*${filter}.*/ OR "to" =~ /.*${filter}.*/ OR call_sid = $filter) `;
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  return sql;
};

/* for Account */
const createCdrQuery = ({page, page_size, trunk, direction, answered, filter, days, start, end}) => {
  let sql = 'SELECT * from cdrs WHERE account_sid = $account_sid ';
  if (trunk) sql += 'AND trunk = $trunk ';
  if (direction) sql += 'AND direction = $direction ';
  if (['true', 'false'].includes(answered)) sql += 'AND answered = $answered ';
  if (filter) sql += `AND ("from" =~ /.*${filter}.*/ OR "to" =~ /.*${filter}.*/ OR call_sid = $filter) `;
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};
const createCdrCountQuery = ({trunk, direction, answered, filter, days, start, end}) => {
  let sql = 'SELECT COUNT(sip_callid) from cdrs WHERE account_sid = $account_sid ';
  if (trunk) sql += 'AND trunk = $trunk ';
  if (direction) sql += 'AND direction = $direction ';
  if (['true', 'false'].includes(answered)) sql += 'AND answered = $answered ';
  if (filter) sql += `AND ("from" =~ /.*${filter}.*/ OR "to" =~ /.*${filter}.*/ OR call_sid = $filter) `;
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  return sql;
};

/* for Service Provider */
const createAlertsQuerySP = ({target_sid, alert_type, page, page_size, days, start, end}) => {
  // eslint-disable-next-line max-len
  let sql = 'SELECT service_provider_sid, message, detail FROM alerts WHERE service_provider_sid = $service_provider_sid ';
  if (target_sid) sql += 'AND target_sid = $target_sid ';
  if (alert_type) sql += 'AND alert_type = $alert_type ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};

const createAlertsCountQuerySP = ({target_sid, alert_type, days, start, end}) => {
  let sql = 'SELECT COUNT(message) FROM alerts WHERE service_provider_sid = $service_provider_sid ';
  if (target_sid) sql += 'AND target_sid = $target_sid ';
  if (alert_type) sql += 'AND alert_type = $alert_type ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  return sql;
};

/* for Account */
const createAlertsQuery = ({target_sid, alert_type, page, page_size, days, start, end}) => {
  let sql = 'SELECT * FROM alerts WHERE account_sid = $account_sid ';
  if (target_sid) sql += 'AND target_sid = $target_sid ';
  if (alert_type) sql += 'AND alert_type = $alert_type ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
  sql += ' ORDER BY time DESC';
  if (page_size) sql += ' LIMIT $page_size';
  if (page) sql += ' OFFSET $offset';
  return sql;
};

const createAlertsCountQuery = ({target_sid, alert_type, days, start, end}) => {
  let sql = 'SELECT COUNT(message) FROM alerts WHERE account_sid = $account_sid ';
  if (target_sid) sql += 'AND target_sid = $target_sid ';
  if (alert_type) sql += 'AND alert_type = $alert_type ';
  if (days) sql += 'AND time > $timestamp ';
  else {
    if (start) sql += 'AND time >= $start ';
    if (end) sql += 'AND time <= $end ';
  }
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
  const {service_provider_sid, account_sid, ...fields} = count;
  const data = {
    measurement: 'call_counts',
    fields,
    tags: {
      service_provider_sid,
      account_sid
    }
  };
  client.locals.data = [...client.locals.data, ...[data]];
  if (client.locals.data.length >= client.locals.commitSize) {
    await writeData(client);
  }
  return;
};

const writeCallCountSP = async(client, count) => {
  if (!client.locals.initialized) await initDatabase(client, 'sp_call_counts');
  const {service_provider_sid, ...fields} = count;
  const data = {
    measurement: 'sp_call_counts',
    fields,
    tags: {
      service_provider_sid
    }
  };
  client.locals.data = [...client.locals.data, ...[data]];
  if (client.locals.data.length >= client.locals.commitSize) {
    await writeData(client);
  }
  return;
};

const writeCallCountApp = async(client, count) => {
  if (!client.locals.initialized) await initDatabase(client, 'app_call_counts');
  const {application_sid, service_provider_sid, account_sid, ...fields} = count;
  const data = {
    measurement: 'app_call_counts',
    fields,
    tags: {
      service_provider_sid,
      account_sid,
      application_sid
    }
  };
  client.locals.data = [...client.locals.data, ...[data]];
  if (client.locals.data.length >= client.locals.commitSize) {
    await writeData(client);
  }
  return;
};

const queryCallCountsApp = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'app_call_counts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createCallCountsCountQueryApp(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`sqlTotal: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCallCountsQueryApp(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
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

const queryCallCountsSP = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'sp_call_counts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createCallCountsCountQuerySP(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`sqlTotal: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCallCountsQuerySP(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
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

const queryCallCounts = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'call_counts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createCallCountsCountQuery(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`sqlTotal: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCallCountsQuery(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
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
      const {direction, host, trunk, service_provider_sid, account_sid, answered, attempted_at, ...fields} = cdr;
      return {
        measurement: 'cdrs',
        timestamp: new Date(attempted_at),
        fields,
        tags: {
          direction,
          host,
          trunk,
          service_provider_sid,
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

const writeSystemAlerts = async(client, systemAlerts) => {
  if (!client.locals.initialized) await initDatabase(client, 'system_alerts');
  systemAlerts = (Array.isArray(systemAlerts) ? systemAlerts : [systemAlerts])
    .map((systemAlert) => {
      const {system_component, state, fields} = systemAlert;
      return {
        measurement: 'system_alerts',
        timestamp: new Date(),
        fields,
        tags: {
          system_component,
          state
        }
      };
    });
  client.locals.data = [...client.locals.data, ...systemAlerts];
  await writeData(client);
  return;
};
const queryCdrsSP = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'cdrs');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createCdrCountQuerySP(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`sql: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCdrQuerySP(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
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

const queryCdrs = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'cdrs');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createCdrCountQuery(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`sql: ${sqlTotal}, results: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createCdrQuery(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
  //console.log(JSON.stringify(res.results[0]));
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
      const {
        alert_type,
        service_provider_sid,
        account_sid,
        target_sid,
        url,
        status,
        vendor,
        count,
        detail,
        timestamp
      } = alert;
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
          case AlertType.ACCOUNT_CALL_LIMIT:
            message = `you have exceeded your account call limit of ${count}; please consider upgrading your plan`;
            break;
          case AlertType.ACCOUNT_DEVICE_LIMIT:
            message =
              // eslint-disable-next-line max-len
              `you have exceeded your account limit of ${count} registered devices; please consider upgrading your plan`;
            break;
          case AlertType.ACCOUNT_API_LIMIT:
            message = `you have exceeded your account api limit of ${count}; please consider upgrading your plan`;
            break;
          case AlertType.SP_CALL_LIMIT:
            // eslint-disable-next-line max-len
            message = `you have exceeded your service provider call limit of ${count}; please consider upgrading your plan`;
            break;
          case AlertType.SP_DEVICE_LIMIT:
            message =
              // eslint-disable-next-line max-len
              `you have exceeded your service provider limit of ${count} registered devices; please consider upgrading your plan`;
            break;
          case AlertType.SP_API_LIMIT:
            // eslint-disable-next-line max-len
            message = `you have exceeded your service provider api limit of ${count}; please consider upgrading your plan`;
            break;
          case AlertType.PLAY_FILENOTFOUND:
            message = `The file at ${url} was not found`;
            break;
          case AlertType.TTS_STREAMING_CONNECTION_FAILURE:
            message = `Failed to connect to tts streaming service at ${vendor}`;
            break;
          default:
            break;
        }
      }
      let fields =  { message };
      if (target_sid) fields = Object.assign(fields, {target_sid});
      const obj = {measurement: 'alerts', fields: fields, tags: { alert_type, service_provider_sid, account_sid,
        ...(vendor && {vendor})}};
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

const generateBindParameters = (opts) => {
  const params = {...opts};
  if (opts.days) params.timestamp = Date.now() * 1000000 - opts.days * 24 * 60 * 60 * 1000000000;
  if (opts.page) params.offset = opts.page  - 1 >= 0 && opts.page_size >= 0 ? (opts.page  - 1) * opts.page_size : 0;
  if (opts.page_size) params.page_size = parseInt(opts.page_size);
  return params;
};

const queryAlertsSP = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'alerts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createAlertsCountQuerySP(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`query total alerts: ${sqlTotal}: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createAlertsQuerySP(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
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

const queryAlerts = async(client, opts) => {
  if (!client.locals.initialized) await initDatabase(client, 'alerts');
  const response = {
    total: 0,
    page_size: opts.page_size,
    page: opts.page,
    data: []
  };
  const params = generateBindParameters(opts);
  const sqlTotal = createAlertsCountQuery(opts);
  const obj = await client.queryRaw(sqlTotal, { placeholders: params});
  //console.log(`query total alerts: ${sqlTotal}: ${JSON.stringify(obj)}`);
  if (!obj.results || !obj.results[0].series) return response;
  response.total = obj.results[0].series[0].values[0][1];

  const sql = createAlertsQuery(opts);
  const res = await client.queryRaw(sql, { placeholders: params});
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

  const cdrClient = new Influx.InfluxDB({database: 'cdrs', schemas: schemas.cdrs, ...opts});
  const alertClient = new Influx.InfluxDB({database: 'alerts', schemas: schemas.alerts, ...opts});
  const callCountClient = new Influx.InfluxDB({database: 'call_counts', schemas: schemas.call_counts, ...opts});
  const callCountSPClient = new Influx.InfluxDB({database: 'sp_call_counts', schemas: schemas.sp_call_counts, ...opts});
  // eslint-disable-next-line max-len
  const callCountAppClient = new Influx.InfluxDB({database: 'app_call_counts', schemas: schemas.app_call_counts, ...opts});
  const systemAlertClient = new Influx.InfluxDB({database: 'system_alerts', schemas: schemas.system_alerts, ...opts});

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
  callCountAppClient.locals = {
    db: 'app_call_counts',
    initialized: false,
    writing: false,
    commitSize: opts.commitSize || 1,
    commitInterval: opts.commitInterval || 10,
    data: []
  };
  callCountSPClient.locals = {
    db: 'sp_call_counts',
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
  systemAlertClient.locals = {
    db: 'system_alerts',
    initialized: false,
    writing: false,
    commitSize: opts.commitSize || 1,
    commitInterval: opts.commitInterval || 10,
    data: []
  };

  if (opts.commitSize > 1 && opts.commitInterval && opts.commitInterval > 2) {
    setInterval(writeData.bind(null, callCountClient), opts.commitInterval * 1000);
    setInterval(writeData.bind(null, callCountSPClient), opts.commitInterval * 1000);
    setInterval(writeData.bind(null, callCountAppClient), opts.commitInterval * 1000);
    setInterval(writeData.bind(null, cdrClient), opts.commitInterval * 1000);
    setInterval(writeData.bind(null, alertClient), opts.commitInterval * 1000);
  }

  return {
    writeCallCount: writeCallCount.bind(null, callCountClient),
    writeCallCountApp: writeCallCountApp.bind(null, callCountAppClient),
    writeCallCountSP: writeCallCountSP.bind(null, callCountSPClient),
    queryCallCounts: queryCallCounts.bind(null, callCountClient),
    queryCallCountsApp: queryCallCountsApp.bind(null, callCountAppClient),
    queryCallCountsSP: queryCallCountsSP.bind(null, callCountSPClient),
    writeCdrs: writeCdrs.bind(null, cdrClient),
    queryCdrsSP: queryCdrsSP.bind(null, cdrClient),
    queryCdrs: queryCdrs.bind(null, cdrClient),
    writeAlerts: writeAlerts.bind(null, alertClient),
    queryAlerts: queryAlerts.bind(null, alertClient),
    queryAlertsSP: queryAlertsSP.bind(null, alertClient),
    writeSystemAlerts: writeSystemAlerts.bind(null, systemAlertClient),
    AlertType: { ...AlertType }
  };
};
