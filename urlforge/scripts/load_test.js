import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const ShortenLatency = new Trend('shorten_duration_ms');
const RedirectLatency = new Trend('redirect_duration_ms');
const ErrorRate = new Rate('error_rate');

export const options = {
  stages: [
    { duration: '30s', target: 50 },   // Warm-up
    { duration: '1m', target: 200 },    // Ramp-up
    { duration: '2m', target: 500 },    // Peak load
    { duration: '30s', target: 0 },     // Ramp-down
  ],
  thresholds: {
    'http_req_duration': ['p(95)<30', 'p(99)<80'], // 95% of requests must finish within 30ms
    'error_rate': ['rate<0.01'],                    // Error rate under 1%
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost';
let sampleCodes = ['gAx7kP2', 'alpha1', 'beta2', 'prod3'];

export default function () {
  const isWrite = Math.random() < 0.1; // 10% writes, 90% reads (industry standard read-heavy ratio)

  if (isWrite) {
    // 1. Shorten URL
    const payload = JSON.stringify({
      original_url: `https://example.com/target-page-${Math.floor(Math.random() * 100000)}`,
      expires_in_days: 30,
    });

    const params = {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'POST_shorten' },
    };

    const res = http.post(`${BASE_URL}/api/v1/shorten`, payload, params);
    ShortenLatency.add(res.timings.duration);

    const success = check(res, {
      'shorten status is 201': (r) => r.status === 201,
      'has short_code': (r) => JSON.parse(r.body).short_code !== undefined,
    });

    if (success) {
      try {
        const body = JSON.parse(res.body);
        sampleCodes.push(body.short_code);
        if (sampleCodes.length > 50) {
          sampleCodes.shift();
        }
      } catch (e) {}
    } else {
      ErrorRate.add(1);
    }
  } else {
    // 2. Redirect URL (Read Path)
    const randomCode = sampleCodes[Math.floor(Math.random() * sampleCodes.length)];
    const params = {
      redirects: 0, // Do not follow redirect automatically, check 302
      tags: { name: 'GET_redirect' },
    };

    const res = http.get(`${BASE_URL}/${randomCode}`, params);
    RedirectLatency.add(res.timings.duration);

    const success = check(res, {
      'redirect status is 302 or 404': (r) => r.status === 302 || r.status === 404,
    });

    if (!success) {
      ErrorRate.add(1);
    } else {
      ErrorRate.add(0);
    }
  }

  sleep(0.05); // 50ms user think time
}
