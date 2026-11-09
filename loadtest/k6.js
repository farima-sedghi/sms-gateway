// Load test: one heavy customer sending normal messages at a high constant
// rate, while 100 other customers send express messages at a steady rate.
//
// MODE=capacity  the heavy customer gets a high rate-limit plan (setup PUTs it),
//                so we measure system throughput and latency.
// MODE=fairness  the heavy customer stays on the default plan (200 msg/s) and
//                must get 429s, while express latency stays unchanged.
//
// Run with loadtest/run.sh (it also computes delivery latency from Postgres).
import http from 'k6/http';
import { check, fail } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const env = (name, def) => (__ENV[name] !== undefined && __ENV[name] !== '' ? __ENV[name] : def);
const num = (name, def) => parseInt(env(name, def), 10);

const BASE_URL = env('BASE_URL', 'http://api:8080');
const MODE = env('MODE', 'capacity');
const RUN_ID = num('RUN_ID', Math.floor(Date.now() / 1000));
const DURATION_S = num('DURATION_S', 120);
const HEAVY_RATE = num('HEAVY_RATE', 1000); // req/s, total over the heavy customers
const HEAVY_CUSTOMERS = num('HEAVY_CUSTOMERS', 1);
const HEAVY_PLAN_RPS = num('HEAVY_PLAN_RPS', 5000); // capacity mode only
const EXPRESS_RATE = num('EXPRESS_RATE', 100); // req/s, total over the express customers
const EXPRESS_CUSTOMERS = num('EXPRESS_CUSTOMERS', 100);
const PRICE = num('SMS_PRICE', 100);

// Customer ids are unique per run, so runs never mix and latency.sql can
// select exactly this run's messages: [RUN_ID*1000, RUN_ID*1000 + 999].
const BASE_ID = RUN_ID * 1000;
const heavyID = (i) => BASE_ID + 1 + (i % HEAVY_CUSTOMERS);
const expressID = (i) => BASE_ID + 101 + (i % EXPRESS_CUSTOMERS);

const JSON_HEADERS = { 'Content-Type': 'application/json' };

// Status counters per scenario (202 accepted, 429 limited, anything else).
const heavyAccepted = new Counter('heavy_accepted');
const heavyLimited = new Counter('heavy_limited');
const heavyOther = new Counter('heavy_other');
const expressAccepted = new Counter('express_accepted');
const expressLimited = new Counter('express_limited');
const expressOther = new Counter('express_other');

export const options = {
  setupTimeout: '180s',
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    heavy: {
      executor: 'constant-arrival-rate',
      exec: 'heavy',
      rate: HEAVY_RATE,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: 200,
      maxVUs: 3000,
    },
    express: {
      executor: 'constant-arrival-rate',
      exec: 'express',
      rate: EXPRESS_RATE,
      timeUnit: '1s',
      duration: `${DURATION_S}s`,
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  thresholds: {
    // Express customers are within their plan: every request must be accepted.
    'http_req_failed{scenario:express}': ['rate==0'],
    // Listed so the summary shows per-scenario latency (limits are generous on purpose).
    'http_req_duration{scenario:express}': ['p(99)<5000'],
    'http_req_duration{scenario:heavy}': ['p(99)<60000'],
    // Fairness mode: the heavy customer must actually be throttled.
    heavy_limited: MODE === 'fairness' ? ['count>0'] : ['count>=0'],
    heavy_accepted: ['count>=0'],
    heavy_other: ['count>=0'],
    express_accepted: ['count>=0'],
    express_limited: ['count==0'],
    express_other: ['count==0'],
  },
};

export function setup() {
  // Enough balance that no message is rejected with 402 during the run.
  const heavyAmount = Math.ceil((HEAVY_RATE * DURATION_S * PRICE * 1.5) / HEAVY_CUSTOMERS);
  const expressAmount = Math.ceil((EXPRESS_RATE * DURATION_S * PRICE * 1.5) / EXPRESS_CUSTOMERS);

  const topUp = (id, amount) => {
    const r = http.post(`${BASE_URL}/customers/${id}/topup`, JSON.stringify({ amount }), { headers: JSON_HEADERS });
    if (r.status !== 200) fail(`top-up for ${id} failed: ${r.status} ${r.body}`);
  };
  for (let i = 0; i < HEAVY_CUSTOMERS; i++) {
    topUp(heavyID(i), heavyAmount);
    if (MODE === 'capacity') {
      const plan = { normal_rps: HEAVY_PLAN_RPS, express_rps: 100, burst: HEAVY_PLAN_RPS * 2 };
      const r = http.put(`${BASE_URL}/customers/${heavyID(i)}/limits`, JSON.stringify(plan), { headers: JSON_HEADERS });
      if (r.status !== 200) fail(`plan for ${heavyID(i)} failed: ${r.status} ${r.body}`);
    }
  }
  for (let i = 0; i < EXPRESS_CUSTOMERS; i++) topUp(expressID(i), expressAmount);
  console.log(`mode=${MODE} run_id=${RUN_ID} customers=[${BASE_ID}, ${BASE_ID + 999}]`);
}

function send(customerID, priority, onAccepted, onLimited, onOther) {
  const it = exec.scenario.iterationInTest;
  const body = JSON.stringify({
    customer_id: customerID,
    recipient: '+98912' + String(1000000 + (it % 9000000)),
    body: `load test ${RUN_ID} ${it}`,
    priority,
  });
  const res = http.post(`${BASE_URL}/messages`, body, {
    headers: { ...JSON_HEADERS, 'Idempotency-Key': `${RUN_ID}-${exec.scenario.name}-${it}` },
    // 429 is an expected answer (it is the rate limiter working), not an error.
    responseCallback: http.expectedStatuses(202, 429),
  });
  if (res.status === 202) onAccepted.add(1);
  else if (res.status === 429) onLimited.add(1);
  else onOther.add(1);
  check(res, { 'accepted or rate limited': (r) => r.status === 202 || r.status === 429 });
}

export function heavy() {
  send(heavyID(exec.scenario.iterationInTest), 'normal', heavyAccepted, heavyLimited, heavyOther);
}

export function express() {
  send(expressID(exec.scenario.iterationInTest), 'express', expressAccepted, expressLimited, expressOther);
}
