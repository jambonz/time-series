const debug = require('debug')('jambonz:time-series');
const assert = require('assert');
const Influx = require('influx');
const schemas = {
  cdrs: {
    measurement: 'cdrs',
    fields: {
      call_sid: Influx.FieldType.STRING,
      from: Influx.FieldType.STRING,
      to: Influx.FieldType.STRING,
      answered: Influx.FieldType.BOOLEAN,
      sip_callid: Influx.FieldType.STRING,
      sip_status: Influx.FieldType.INTEGER,
      duration: Influx.FieldType.INTEGER,
      attempted_at: Influx.FieldType.INTEGER,
      answered_at: Influx.FieldType.INTEGER,
      terminated_at: Influx.FieldType.INTEGER,
      termination_reason: Influx.FieldType.STRING,
      remote_host: Influx.FieldType.STRING
    },
    tags: [
      'account_sid',
      'host',
      'trunk',
      'direction'
    ]
  },
  alerts: {
    measurement: 'alerts',
    fields: {
      url: Influx.FieldType.STRING,
      vendor: Influx.FieldType.STRING,
      message: Influx.FieldType.STRING
    },
    tags: [
      'account_sid',
      'alert_type'
    ]
  }
};

const createCdrQuery = ({account_sid, trunk, limit}) => {
  let sql = 'select * from cdrs ';
  const filters = [];
  if (account_sid) filters.push({key: 'account_sid', value: account_sid});
  if (trunk) filters.push({key: 'trunk', value: trunk});
  if (filters.length) {
    sql += 'where ';
    sql += filters.map((f) => `${f.key} = '${f.value}'`).join(' AND ');
  }
  sql += ' order by time desc ';
  if (limit) sql += ` limit ${limit}`;
  return sql;
};

const createAlertQuery = ({account_sid, alert_type, limit}) => {
  let sql = 'select * from alerts ';
  const filters = [];
  if (account_sid) filters.push({key: 'account_sid', value: account_sid});
  if (alert_type) filters.push({key: 'alert_type', value: alert_type});
  if (filters.length) {
    sql += 'where ';
    sql += filters.map((f) => `${f.key} = '${f.value}'`).join(' AND ');
  }
  sql += ' order by time desc ';
  if (limit) sql += ` limit ${limit}`;
  debug(`createAlertQuery: ${sql}`);
  return sql;
};

const initDatabase = async(client, dbName) => {
  const names = await client.getDatabaseNames();
  if (!names.includes(dbName)) {
    await client.createDatabase(dbName);
  }
  client._initialized = true;
};

const writeCdrs = async(client, cdrs) => {
  if (!client._initialized) await initDatabase(client, 'cdrs');
  cdrs = (Array.isArray(cdrs) ? cdrs : [cdrs])
    .map((cdr) => {
      const {direction, host, trunk, account_sid, ...fields} = cdr;
      return {
        measurement: 'cdrs',
        fields,
        tags: {
          direction,
          host,
          trunk,
          account_sid
        }
      };
    });
  debug(`writing cdrs: ${JSON.stringify(cdrs)}`);
  return await client.writePoints(cdrs);
};

const queryCdrs = async(client, opts) => {
  if (!client._initialized) await initDatabase(client, 'alerts');
  const sql = createCdrQuery(opts);
  return await client.queryRaw(sql);
};

const writeAlerts = async(client, alerts) => {
  if (!client._initialized) await initDatabase(client, 'alerts');
  alerts = (Array.isArray(alerts) ? alerts : [alerts])
    .map((alert) => {
      const {alert_type, account_sid, ...fields} = alert;
      return {
        measurement: 'alerts',
        fields,
        tags: {
          alert_type,
          account_sid
        }
      };
    });
  debug(`writing alerts: ${JSON.stringify(alerts)}`);
  return await client.writePoints(alerts);
};

const queryAlerts = async(client, opts) => {
  if (!client._initialized) await initDatabase(client, 'alerts');
  const sql = createAlertQuery(opts);
  return await client.queryRaw(sql);
};

module.exports = (logger, opts) => {
  if (typeof opts === 'string') opts = {host: opts};
  assert(opts.host);

  const cdrClient = new Influx.InfluxDB({database: 'cdrs', schemas: schemas.cdr, ...opts});
  const alertClient = new Influx.InfluxDB({database: 'alerts', schemas: schemas.alerts, ...opts});
  cdrClient._initialized = false;
  alertClient._initialized = false;

  return {
    writeCdrs: writeCdrs.bind(null, cdrClient),
    queryCdrs: queryCdrs.bind(null, cdrClient),
    writeAlerts: writeAlerts.bind(null, alertClient),
    queryAlerts: queryAlerts.bind(null, alertClient),
    AlertType: {
      WEBHOOK_FAILURE: 'webhook-failure',
      TTS_NOT_PROVISIONED: 'no-tts',
      STT_NOT_PROVISIONED: 'no-stt',
      CARRIER_NOT_PROVISIONED: 'no-carrier',
      CALL_LIMIT: 'call-limit',
      DEVICE_LIMIT: 'device-limit',
      API_LIMIT: 'api-limit'
    }
  };
};
