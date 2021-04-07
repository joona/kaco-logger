const got = require('got');

const KACO_URL = process.env.KACO_URL;
const MQTT_URL = process.env.MQTT_URL;
const MQTT_ENABLED = true;

const TIMEOUT_POLL_REALTIME = parseInt(process.env.INTERVAL_REALTIME || 5) * 1000;
const TIMEOUT_POLL_TOTAL = parseInt(process.env.INTERVAL_TOTAL || 60) * 1000;

const TYPE_TODAY = 'today';

const METRIC_CURRENT_POWER = 'current-power';
const METRIC_CURRENT_POWER_KW = 'current-power-kilowatts';
const METRIC_DAILY_TOTAL_POWER = 'daily-total-power';
const METRIC_TOTAL_POWER = 'total-power';

const SOURCE_NAME = process.env.SOURCE_NAME || 'pv';
const GAUGE_DAILY_TOTAL_POWER = 'daily';
const GAUGE_TOTAL_POWER = 'total';
const GAUGE_CURRENT_POWER = 'current_power';
const GAUGE_CURRENT_POWER_KW = 'current_power_kw';

const mqtt = require('mqtt').connect(MQTT_URL);

const context = {
  total: {}
};

function round(x, n) {
  return (Math.round(x * 100) / 100);
}

function pad(string, char, totalLength) {
  string = String(string);
  const stringLength = string.length;
  let pre = '';

  for (var i = 0, len = totalLength - stringLength; i < len; i++) {
    pre += char;
  }

  return pre + string;
}

/*
 *
 */
function parseMetadata(data) {
   /*
    0.)     Serial
    1.)     Device type
    2.)     MAC address
    3.)     IP address                      (IPv4)
    4.)     Inverter RS485 address          (Range 1-31)
    5.)     Anzahl AC-Phasen                (Range 1-3)
    6.)     DC entries                      (Range 1-3)
    7.)     AC nominal power                [W]
    8.)     AC voltage phases monitoring    (1: Show 1 phase voltage; 3: Show 3 phase voltages)
    9.)     AC current phases monitoring    (1: Show 1 phase current; 3: Show 3 phase currents)
    10.)    Show AC phases monitoring mode  (0: Hide; 1: Show)        
    11.)    Language ID                     (ISO 639)
    12.)    SW packet version               (for example 101 for 1.01)
    13.)    ARM SW-Version                  (for example 101 for 1.01)
    14.)    ARM SW-Version checksum
    15.)    CFG SW-Version                  (for example 10001 for 1.0001)
    16.)    CFG SW-Version checksum       
    17.)    DSP SW-Version                  (for example 123 for 1.23)
    18.)    DSP SW-Version checksum
    19.)    PIC SW-Version                  (for example 123 for 1.23)
    20.)    PIC SW-Version checksum
    21.)    Vendor name                     (for example 'KACO', 'SCHUECO', ...)
    22.)    WebConfig active/inactive
    */

  const fields = data.split(';');
  context.serial = fields[0];
  context.deviceType = fields[1];
  context.ipAddress = fields[3];
  context.dcCount = parseInt(fields[6]);
  context.phaseCount = parseInt(fields[5]);
}

function parseRealtimeData(data) {
  const fields = data.split(';');
  const [ rDate ] = fields;

  const obj = {
    generators: []
  }

  const rawEpoch = context.lastFetch = rDate*1000;
  const tDate = obj.ts = new Date(rawEpoch);

  for (var t = 1; t <= context.dcCount; t++) 
  {
    const voltage = (fields[t] / (65535.0 / 1600.0));
    const current = ((fields[t + context.dcCount + context.phaseCount]) / (65535.0 / 200.0));
    const production = round((voltage * current) / 1000.0, 2);
    console.log(`String[${t}]`, `${production} kWh,`, `${round(voltage, 2)}V,`, `${round(current, 2)}A`);
    //obj.generators.push(value);
    obj.generators.push({
      generator: t,
      voltage,
      current,
      production
    });

    //gen.push(roundCommaSeparated(( *  / 1000.0), 2));
  }

  const nowInRaw = fields[fields.length - 3];
  const nowInValue = round(nowInRaw / (65535.0 / 100000.0) / 1000.0, 2);
  console.log('Yield', nowInValue, 'kWh');
  //console.log('Data:', obj);
  console.log();

  obj.power = nowInValue * 1000;
  return obj;
}

function parseData(raw, type, timestamp) {
  const lines = raw.split('\r');
  // Skip the first 3 lines (contain meta data and column headers)
  const values = lines.slice(3).map(x => x.split(';'));

  switch(type) {
    case TYPE_TODAY:
      return parseDay(values, timestamp);
  }

  return null;
}

function parseDay(rows, ts) {
  const items = [];
  let acTotalPower = 0;
  let dcTotalPower = 0;

  for (var i = 0, len = rows.length - 1; i < len; i++) {
    const row = rows[i];
    if(row[0] === 0) continue;

    const [ hours, minutes, seconds ] = row[0].split(':');
    const date = new Date(ts.getFullYear(), ts.getMonth(), ts.getDay(), hours, minutes, seconds);
    date.setHours(date.getHours() - (date.getTimezoneOffset() / 60));

    if(context.dcCount !== 2) {
      throw new Error('invalid data structure');
    }

    const [ 
      foo, 
      dcVoltage1, dcCurrent1, dcPower1,
      dcVoltage2, dcCurrent2, dcPower2,
      acVoltage1, acCurrent1,
      acVoltage2, acCurrent2,
      acVoltage3, acCurrent3,
      dcPowerTotal, acPowerTotal,
      temperature
    ] = row.map(x => parseFloat(String(x).replace(',', '.')));

    const item = {
      ts: date,
      dcVoltage1, dcVoltage2,
      dcCurrent1, dcCurrent2,
      dcPower1: dcPower1 / 1000, 
      dcPower2: dcPower2 / 1000,
      acVoltage1, acVoltage2, acVoltage3,
      acCurrent1, acCurrent2, acCurrent3,
      acPowerTotal: acPowerTotal / 1000, 
      dcPowerTotal: dcPowerTotal / 1000
    };

    items.push(item);
  }

  let diff = 5;
  for (var y = 0, len = items.length; y < len; y++) {
    const x = items[y];
    if(y > 0) {
      diff = (x.ts.getTime() - items[y-1].ts.getTime()) / 60000;
    }

    acTotalPower += (x.acPowerTotal * diff);
  }

  return {
    items,
    total: {
      acPower: round(acTotalPower / 60.0, 2),
      dcPower: dcTotalPower
    }
  }
}

function printData() {
  console.log('Context:', context);
}


async function fetchToday() {
  const now = new Date();
  //const today = [now.getFullYear(), now.getUTCMonth(), now.getUTCDay()].join('');

  const [ year, month, day ] = [ now.getFullYear(), now.getMonth(), now.getDate() ];
  const today = [ year, pad(month+1, '0', 2), pad(day, '0', 2) ].join('');

  const raw = await got(`${KACO_URL}/${today}.csv`)
    .then(response => response.body);

  const data = parseData(raw, TYPE_TODAY, now);
  console.log('data:', data.total);

  const { total } = data;
  context.total.daily = total.acPower;
  context.lastTotalFetch = new Date();

  console.log(`Total production today: ${total.acPower} kWh`);

  publish(METRIC_DAILY_TOTAL_POWER, {
    ts: context.lastTotalFetch,
    value: (context.total.daily)
  });
}

async function fetchEternalTotal() {
  const raw = await got(`${KACO_URL}/eternal.csv`)
    .then(response => response.body);

  const lines = raw.split('\r');
  const totalRow = lines[1].split(';');
  const eternalTotal = parseFloat(totalRow[4]);
  
  context.total.eternal = eternalTotal;
  context.lastEternalFetch = new Date();

  publish(METRIC_TOTAL_POWER, {
    ts: context.lastEternalFetch,
    value: (eternalTotal)
  });
}

async function fetchMetadata() {
  return got(`${KACO_URL}/meta.csv`)
    .then(response => {
      return parseMetadata(response.body);
    });
}

async function fetchRealtime() {
  return got(`${KACO_URL}/realtime.csv?_=${context.lastFetch || 0}`)
    .then(response => {
      const data = parseRealtimeData(response.body);

      publish(METRIC_CURRENT_POWER, {
        ts: data.ts,
        value: data.power
      });

      const kw = data.power / 1000;
      publish(METRIC_CURRENT_POWER_KW, {
        ts: data.ts,
        value: kw
      });
    });
}

function getTopic(metricType) {
  switch(metricType) {
    case METRIC_CURRENT_POWER:
      return `production/${SOURCE_NAME}/${GAUGE_CURRENT_POWER}`;
    case METRIC_CURRENT_POWER_KW:
      return `production/${SOURCE_NAME}/${GAUGE_CURRENT_POWER_KW}`;
    case METRIC_DAILY_TOTAL_POWER:
      return `production/${SOURCE_NAME}/${GAUGE_DAILY_TOTAL_POWER}`;
    case METRIC_TOTAL_POWER:
      return `production/${SOURCE_NAME}/${GAUGE_TOTAL_POWER}`;
  }
}

async function publish(metricType, data) {
  const { ts, value } = data;
  const topic = getTopic(metricType);

  console.log('publish', topic, data);
  
  return mqtt.publish(topic, String(round(value)));
}

function pollRealtime() {
  clearTimeout(context.timerPoll);
  fetchRealtime()
    .then(resetRealtimePoll)
    .catch(resetRealtimePoll);
}

function resetRealtimePoll() {
  context.timerPoll = setTimeout(() => {
    pollRealtime();
  }, TIMEOUT_POLL_REALTIME);
}

function resetTotalPoll() {
  context.timerPollTotal = setTimeout(() => {
    pollTotal();
  }, TIMEOUT_POLL_TOTAL);
}

function pollTotal() {
  clearTimeout(context.timerPollTotal);
  fetchToday()
    .then(fetchEternalTotal)
    .then(resetTotalPoll)
    .catch(resetTotalPoll);
}

async function main() {
  const command = process.argv[2] || 'poll';

  if(!KACO_URL) {
    throw new Error(`KACO_URL is required!`);
  }

  if(!MQTT_URL) {
    throw new Error(`MQTT_URL is required!`);
  }

  await fetchMetadata();
  printData();

  switch(command) {
    case 'poll':
      await fetchRealtime();
      pollRealtime();
      pollTotal();
      break;

    case 'yield:today':
      await fetchToday();
      break;

    case 'yield:eternal':
      await fetchEternalTotal();
      break;
  }
  
}

if(!module.parent) {
  main()
    .catch(err => {
      console.error('Unexpected error:', err.message);
      console.error(err.stack);
    });
}
