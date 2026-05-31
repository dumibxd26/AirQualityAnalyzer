// AirQualityAnalyzer dashboard logic.
// Connects to the FastAPI websocket and renders live state.

const PARAM_THRESHOLDS = {
  pm25: 15, pm10: 45, no2: 25, o3: 60, so2: 40, co: 4,
};

// Per-parameter readings on the map are considered stale after this. Older
// entries are ignored when computing a station's severity color, so a single
// historical spike can't keep a dot red forever.
const STATION_PARAM_TTL_MS = 10 * 60 * 1000; // 10 minutes
const SEVERITY_RANK = ['critical', 'high', 'moderate', 'good']; // worst -> best

const state = {
  ws: null,
  // location -> {lat, lon, marker, avgBy: {param: {value, ts, source}}}
  // `source` is 'enriched' (Flink window AVG, authoritative) or 'raw'
  // (single sample, used only as a fallback before any window has closed).
  stations: new Map(),
  alerts: [],
  critical: [],
  rateBuffer: [],         // raw-reading timestamps in last 60s
  enrichedBuffer: [],     // enriched-reading timestamps in last 60s
  totalReadings: 0,       // cumulative since this tab loaded
  lastReadingTs: null,    // wall-clock of most recent raw reading
  chartParam: 'pm25',
  chartSeries: [],
  chartMaxSeries: [],
  mapParam: 'all',        // 'all' = worst across pollutants, else a single one
  paused: false,          // freeze live updates for clean screenshots
};

// ----------------- map -----------------
const map = L.map('map', { zoomControl: true, attributionControl: false }).setView([20, 0], 2);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  maxZoom: 19,
  subdomains: 'abcd',
}).addTo(map);

// Keep the Leaflet canvas in sync with its (now fixed-height) container so it
// repaints correctly after the window is resized.
let _resizeRaf = 0;
window.addEventListener('resize', () => {
  cancelAnimationFrame(_resizeRaf);
  _resizeRaf = requestAnimationFrame(() => map.invalidateSize());
});

function severityFor(param, value) {
  const t = PARAM_THRESHOLDS[param] ?? Infinity;
  if (value >= 3 * t) return 'critical';
  if (value >= 2 * t) return 'high';
  if (value >= t)     return 'moderate';
  return 'good';
}

function upsertStation(rec) {
  if (rec.lat == null || rec.lon == null) return;
  const key = rec.location;
  let st = state.stations.get(key);
  if (!st) {
    st = { lat: rec.lat, lon: rec.lon, avgBy: {}, marker: null };
    state.stations.set(key, st);
  }
  // First-wins coordinates: avoid jitter when different sensors at the same
  // station report slightly different lat/lon for different pollutants.
  if (st.lat == null || st.lon == null) {
    st.lat = rec.lat;
    st.lon = rec.lon;
  }

  if (rec.parameter && typeof rec.value === 'number') {
    const prev = st.avgBy[rec.parameter];
    // Don't let a single raw sample overwrite a Flink-windowed average.
    if (!(prev && prev.source === 'enriched' && rec.source !== 'enriched')) {
      st.avgBy[rec.parameter] = {
        value: rec.value,
        ts: Date.now(),
        source: rec.source || 'raw',
      };
    }
  }

  // Worst severity across NON-STALE parameters. When a specific map pollutant
  // is selected, color by that pollutant alone (stations without a fresh
  // reading for it render as 'good').
  const cutoff = Date.now() - STATION_PARAM_TTL_MS;
  let worst = 'good';
  for (const [p, entry] of Object.entries(st.avgBy)) {
    if (entry.ts < cutoff) continue;
    if (state.mapParam !== 'all' && p !== state.mapParam) continue;
    const s = severityFor(p, entry.value);
    if (SEVERITY_RANK.indexOf(s) < SEVERITY_RANK.indexOf(worst)) {
      worst = s;
    }
  }

  const html = `<div class="station-dot ${worst}"></div>`;
  const icon = L.divIcon({ html, className: '', iconSize: [14, 14] });
  if (!st.marker) {
    st.marker = L.marker([st.lat, st.lon], { icon }).addTo(map);
  } else {
    st.marker.setIcon(icon);
  }
  st.marker.bindPopup(stationPopup(key, st));
  document.getElementById('stat-stations').textContent = state.stations.size;
}

function stationPopup(name, st) {
  const cutoff = Date.now() - STATION_PARAM_TTL_MS;
  const rows = Object.entries(st.avgBy)
    .filter(([, e]) => e.ts >= cutoff)
    .map(([p, e]) => {
      const tag = e.source === 'enriched' ? 'avg' : '~';
      return `<div><b>${p}</b>: ${Number(e.value).toFixed(2)} <span style="color:var(--muted)">(${tag})</span></div>`;
    }).join('');
  return `<div><b>${name}</b></div>${rows || '<div>no data yet</div>'}`;
}

// Periodically force a redraw so dots fade back to green as their per-param
// entries age past the TTL even if no new data arrives for that station.
setInterval(() => {
  state.stations.forEach((st, key) => {
    upsertStation({ location: key, lat: st.lat, lon: st.lon });
  });
}, 30_000);

// ----------------- chart -----------------
function makeLineChart(canvasId, color, fillRgba) {
  return new Chart(document.getElementById(canvasId), {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'value',
        data: [],
        borderColor: color,
        backgroundColor: ctx => {
          const c = ctx.chart.ctx;
          const area = ctx.chart.chartArea;
          const h = area ? area.bottom : 280;
          const g = c.createLinearGradient(0, area ? area.top : 0, 0, h);
          g.addColorStop(0, fillRgba.replace('ALPHA', '0.30'));
          g.addColorStop(1, fillRgba.replace('ALPHA', '0'));
          return g;
        },
        borderWidth: 2.5,
        tension: 0.35,
        fill: true,
        pointRadius: 0,
        pointHoverRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#8b93a7', font: { size: 10 }, maxTicksLimit: 8 }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#8b93a7' }, grid: { color: 'rgba(255,255,255,0.05)' }, beginAtZero: true, grace: '35%' },
      },
      animation: { duration: 400 },
    },
  });
}

const chart    = makeLineChart('chart',     '#6ee7ff', 'rgba(110,231,255,ALPHA)');
const chartMax = makeLineChart('chart-max', '#fb923c', 'rgba(251,146,60,ALPHA)');

const PARAM_LABELS = {
  pm25: 'PM2.5', pm10: 'PM10', no2: 'NO\u2082', o3: 'O\u2083', so2: 'SO\u2082', co: 'CO',
};

function refreshChartHeadings() {
  const label = PARAM_LABELS[state.chartParam] || state.chartParam;
  const t = PARAM_THRESHOLDS[state.chartParam];
  document.querySelector('.chart-panel .panel-head h2').textContent = `${label} \u2014 rolling average`;
  document.querySelector('.chart-max-panel .panel-head h2').textContent = `${label} \u2014 network peak`;
  const hint = document.getElementById('chart-max-hint');
  if (hint && t) hint.textContent = `worst station \u00b7 WHO ${t}`;
}

document.getElementById('param-select').addEventListener('change', e => {
  state.chartParam = e.target.value;
  state.chartSeries = [];
  state.chartMaxSeries = [];
  for (const ch of [chart, chartMax]) {
    ch.data.labels = [];
    ch.data.datasets[0].data = [];
    ch.update('none');
  }
  refreshChartHeadings();
  // re-seed from current in-memory station readings so the charts aren't blank
  const seed = [];
  state.stations.forEach(st => {
    const e2 = st.avgBy[state.chartParam];
    if (e2 && typeof e2.value === 'number') seed.push(e2.value);
  });
  if (seed.length) {
    const avg = seed.reduce((s, v) => s + v, 0) / seed.length;
    const max = seed.reduce((m, v) => Math.max(m, v), -Infinity);
    const now = Date.now();
    pushChartSample(state.chartParam, avg, now);
    pushChartMaxSample(state.chartParam, max, now);
  }
});

function pushChartSample(param, value, ts) {
  if (param !== state.chartParam) return;
  state.chartSeries.push({ ts, value });
  if (state.chartSeries.length > 60) state.chartSeries.shift();
  chart.data.labels = state.chartSeries.map(p => new Date(p.ts).toLocaleTimeString());
  chart.data.datasets[0].data = state.chartSeries.map(p => p.value);
  chart.update('none');
}

function pushChartMaxSample(param, value, ts) {
  if (param !== state.chartParam) return;
  state.chartMaxSeries.push({ ts, value });
  if (state.chartMaxSeries.length > 60) state.chartMaxSeries.shift();
  chartMax.data.labels = state.chartMaxSeries.map(p => new Date(p.ts).toLocaleTimeString());
  chartMax.data.datasets[0].data = state.chartMaxSeries.map(p => p.value);
  chartMax.update('none');
}

refreshChartHeadings();

// ----------------- alerts feed -----------------
const alertList = document.getElementById('alert-list');
const readingsList = document.getElementById('readings-list');
const toastHost = document.getElementById('toast-host');

document.getElementById('clear-alerts').onclick = () => {
  state.alerts = [];
  alertList.innerHTML = '';
  document.getElementById('stat-alerts').textContent = 0;
};

function addAlert(a) {
  state.alerts.unshift(a);
  state.alerts = state.alerts.slice(0, 100);
  document.getElementById('stat-alerts').textContent = state.alerts.length;

  const li = document.createElement('li');
  li.className = `alert-item ${a.severity || 'MODERATE'}`;
  const when = a.window_end ? new Date(a.window_end).toLocaleTimeString() : '';
  li.innerHTML = `
    <div class="bar"></div>
    <div class="meta">
      <div class="top">${a.location} <span style="color:var(--muted);font-weight:400;">· ${a.parameter}</span></div>
      <div class="sub">${a.severity || ''} · ${when}</div>
    </div>
    <div class="value">${Number(a.avg_value).toFixed(2)}</div>
  `;
  alertList.prepend(li);
  while (alertList.children.length > 100) alertList.removeChild(alertList.lastChild);
}

function addCritical(c, showToast = true) {
  state.critical.unshift(c);
  state.critical = state.critical.slice(0, 50);
  document.getElementById('stat-critical').textContent = state.critical.length;

  if (!showToast) return;

  // Keep at most 4 toasts on screen; drop the oldest when a new one arrives.
  while (toastHost.children.length >= 4) toastHost.removeChild(toastHost.firstChild);

  const t = document.createElement('div');
  t.className = 'toast';
  t.innerHTML = `<b>CRITICAL · ${c.location}</b>
    <div>${c.parameter} peaked at <b>${Number(c.peak_value).toFixed(2)}</b> across ${c.breach_count} windows</div>
    <small>${new Date(c.last_window_end).toLocaleString()}</small>`;
  toastHost.appendChild(t);
  setTimeout(() => t.remove(), 9000);
}

function addReading(r) {
  const now = Date.now();
  state.rateBuffer.push(now);
  state.totalReadings += 1;
  state.lastReadingTs = now;

  const li = document.createElement('li');
  li.className = 'fresh';
  li.innerHTML = `
    <span><span class="loc">${r.location}</span><span class="param">${r.parameter}</span></span>
    <span class="val">${Number(r.value).toFixed(2)}</span>
  `;
  readingsList.prepend(li);
  while (readingsList.children.length > 60) readingsList.removeChild(readingsList.lastChild);
  setTimeout(() => li.classList.remove('fresh'), 1500);
}

function worstSeverityLabel() {
  // Count alerts by severity in the last 100 buffered.
  const counts = { CRITICAL: 0, HIGH: 0, MODERATE: 0 };
  for (const a of state.alerts) {
    if (counts[a.severity] != null) counts[a.severity] += 1;
  }
  if (counts.CRITICAL) return `worst: critical (${counts.CRITICAL})`;
  if (counts.HIGH)     return `worst: high (${counts.HIGH})`;
  if (counts.MODERATE) return `worst: moderate (${counts.MODERATE})`;
  return 'worst: —';
}

function formatAge(ms) {
  if (ms == null) return '—';
  const s = Math.floor(ms / 1000);
  if (s < 1)  return 'just now';
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  return `${Math.floor(m / 60)}h ago`;
}

// Stats ticker: tick every 1s so the dashboard never feels frozen.
setInterval(() => {
  const now = Date.now();
  const cutoff60 = now - 60_000;
  const cutoff5  = now - 5_000;
  state.rateBuffer     = state.rateBuffer.filter(t => t > cutoff60);
  state.enrichedBuffer = state.enrichedBuffer.filter(t => t > cutoff60);
  const last5 = state.rateBuffer.filter(t => t > cutoff5).length;

  document.getElementById('stat-rate').textContent     = state.rateBuffer.length;
  document.getElementById('stat-rate-sec').textContent = `${(last5 / 5).toFixed(1)}/s · rolling 60 s`;
  document.getElementById('stat-enriched').textContent = state.enrichedBuffer.length;
  document.getElementById('stat-total').textContent    = state.totalReadings.toLocaleString();
  document.getElementById('stat-last').textContent     =
    state.lastReadingTs ? `last: ${formatAge(now - state.lastReadingTs)}` : 'last: —';
  document.getElementById('stat-worst').textContent    = worstSeverityLabel();
}, 1000);

// Chart aggregator: tick every 5 s and push the network-wide mean of the
// latest non-stale Flink-windowed averages per station. This matches the
// panel title ("rolling average") and gives a dense, meaningful series even
// when only a fraction of stations report the selected parameter each tick.
setInterval(() => {
  const cutoff = Date.now() - STATION_PARAM_TTL_MS;
  const values = [];
  state.stations.forEach(st => {
    const e = st.avgBy[state.chartParam];
    if (!e || e.ts < cutoff || typeof e.value !== 'number') return;
    values.push(e.value);
  });
  if (!values.length) return;
  const avg = values.reduce((s, v) => s + v, 0) / values.length;
  const max = values.reduce((m, v) => Math.max(m, v), -Infinity);
  const now = Date.now();
  pushChartSample(state.chartParam, avg, now);
  pushChartMaxSample(state.chartParam, max, now);
}, 5000);

// ----------------- websocket -----------------
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const url = `${proto}://${location.host}/ws`;
  state.ws = new WebSocket(url);

  state.ws.onopen = () => setConn(true);
  state.ws.onclose = () => { setConn(false); setTimeout(connect, 2000); };
  state.ws.onerror = () => state.ws.close();
  state.ws.onmessage = ev => {
    const msg = JSON.parse(ev.data);
    handle(msg);
  };
}

function setConn(ok) {
  document.getElementById('conn-dot').classList.toggle('offline', !ok);
  document.getElementById('conn-text').textContent = ok ? 'live · streaming' : 'reconnecting…';
}

// Recolor every existing marker using the current map pollutant filter.
function recolorStations() {
  state.stations.forEach((st, key) => {
    upsertStation({ location: key, lat: st.lat, lon: st.lon });
  });
}

// ----- map pollutant selector -----
const mapSelect = document.getElementById('map-param-select');
if (mapSelect) {
  mapSelect.addEventListener('change', e => {
    state.mapParam = e.target.value;
    recolorStations();
  });
}

// ----- pause / freeze button -----
const pauseBtn = document.getElementById('pause-btn');
if (pauseBtn) {
  pauseBtn.addEventListener('click', () => {
    state.paused = !state.paused;
    pauseBtn.classList.toggle('paused', state.paused);
    pauseBtn.textContent = state.paused ? '▶ resume' : '‖ pause';
  });
}

function handle(msg) {
  if (msg.type === 'snapshot') {
    const seedValues = [];
    (msg.data.stations || []).forEach(s => {
      const rec = { location: s.location, lat: s.lat, lon: s.lon };
      upsertStation(rec);
      Object.entries(s.readings || {}).forEach(([p, v]) => {
        upsertStation({
          location: s.location, lat: s.lat, lon: s.lon,
          parameter: p, value: v, source: 'raw',
        });
        if (p === state.chartParam && typeof v === 'number') seedValues.push(v);
      });
    });
    // seed the chart with an initial point so it isn't blank for 5 s
    if (seedValues.length) {
      const avg = seedValues.reduce((s, v) => s + v, 0) / seedValues.length;
      const max = seedValues.reduce((m, v) => Math.max(m, v), -Infinity);
      const now = Date.now();
      pushChartSample(state.chartParam, avg, now);
      pushChartMaxSample(state.chartParam, max, now);
    }
    (msg.data.alerts || []).forEach(addAlert);
    (msg.data.critical || []).forEach(c => addCritical(c, false));
    document.getElementById('stat-alerts').textContent = state.alerts.length;
    document.getElementById('stat-critical').textContent = state.critical.length;
    return;
  }
  if (msg.type === 'reading') {
    // Raw readings keep the station alive on the map (coords, last_seen) and
    // feed the live readings list, but they are NOT authoritative for
    // severity color or the rolling-average chart -- enriched (windowed AVG)
    // events are. The chart is driven by the 5 s aggregator that scans
    // state.stations for the latest enriched values.
    if (state.paused) return;
    upsertStation({ ...msg.data, source: 'raw' });
    addReading(msg.data);
    return;
  }
  if (msg.type === 'enriched') {
    if (state.paused) return;
    state.enrichedBuffer.push(Date.now());
    // Authoritative window-averaged value -> drives map color, in agreement
    // with the alert thresholds Flink applies on the same average.
    upsertStation({
      location: msg.data.location,
      lat: msg.data.lat, lon: msg.data.lon,
      parameter: msg.data.parameter, value: msg.data.avg_value,
      source: 'enriched',
    });
    return;
  }
  if (msg.type === 'alert')    { if (state.paused) return; addAlert(msg.data); return; }
  if (msg.type === 'critical') { if (state.paused) return; addCritical(msg.data); return; }
}

connect();
