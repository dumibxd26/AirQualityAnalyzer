// AirQualityAnalyzer dashboard logic.
// Connects to the FastAPI websocket and renders live state.

const PARAM_THRESHOLDS = {
  pm25: 15, pm10: 45, no2: 25, o3: 60, so2: 40, co: 4,
};

const state = {
  ws: null,
  stations: new Map(), // location -> {lat, lon, marker, lastBy}
  alerts: [],
  critical: [],
  rateBuffer: [], // timestamps of recent readings
  chartParam: 'pm25',
  chartSeries: [],
};

// ----------------- map -----------------
const map = L.map('map', { zoomControl: true, attributionControl: false }).setView([20, 0], 2);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  maxZoom: 19,
  subdomains: 'abcd',
}).addTo(map);

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
    st = { lat: rec.lat, lon: rec.lon, lastBy: {}, marker: null };
    state.stations.set(key, st);
  }
  if (rec.parameter) st.lastBy[rec.parameter] = rec.value;

  // Compute worst severity across parameters.
  let worst = 'good';
  for (const [p, v] of Object.entries(st.lastBy)) {
    const s = severityFor(p, v);
    if (['critical','high','moderate','good'].indexOf(s) <
        ['critical','high','moderate','good'].indexOf(worst)) {
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
  const rows = Object.entries(st.lastBy)
    .map(([p, v]) => `<div><b>${p}</b>: ${Number(v).toFixed(2)}</div>`).join('');
  return `<div><b>${name}</b></div>${rows || '<div>no data yet</div>'}`;
}

// ----------------- chart -----------------
const chartCtx = document.getElementById('chart');
const chart = new Chart(chartCtx, {
  type: 'line',
  data: {
    labels: [],
    datasets: [{
      label: 'avg',
      data: [],
      borderColor: '#6ee7ff',
      backgroundColor: ctx => {
        const c = ctx.chart.ctx;
        const g = c.createLinearGradient(0, 0, 0, 280);
        g.addColorStop(0, 'rgba(110,231,255,0.45)');
        g.addColorStop(1, 'rgba(110,231,255,0)');
        return g;
      },
      borderWidth: 2,
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
      x: { ticks: { color: '#8b93a7', font: { size: 10 } }, grid: { color: 'rgba(255,255,255,0.04)' } },
      y: { ticks: { color: '#8b93a7' }, grid: { color: 'rgba(255,255,255,0.05)' } },
    },
    animation: { duration: 400 },
  },
});

document.getElementById('param-select').addEventListener('change', e => {
  state.chartParam = e.target.value;
  state.chartSeries = [];
  chart.data.labels = [];
  chart.data.datasets[0].data = [];
  chart.update('none');
});

function pushChartSample(param, value, ts) {
  if (param !== state.chartParam) return;
  state.chartSeries.push({ ts, value });
  if (state.chartSeries.length > 60) state.chartSeries.shift();
  chart.data.labels = state.chartSeries.map(p => new Date(p.ts).toLocaleTimeString());
  chart.data.datasets[0].data = state.chartSeries.map(p => p.value);
  chart.update('none');
}

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

function addCritical(c) {
  state.critical.unshift(c);
  state.critical = state.critical.slice(0, 50);
  document.getElementById('stat-critical').textContent = state.critical.length;

  const t = document.createElement('div');
  t.className = 'toast';
  t.innerHTML = `<b>CRITICAL · ${c.location}</b>
    <div>${c.parameter} peaked at <b>${Number(c.peak_value).toFixed(2)}</b> across ${c.breach_count} windows</div>
    <small>${new Date(c.last_window_end).toLocaleString()}</small>`;
  toastHost.appendChild(t);
  setTimeout(() => t.remove(), 9000);
}

function addReading(r) {
  state.rateBuffer.push(Date.now());

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

// readings/min ticker
setInterval(() => {
  const cutoff = Date.now() - 60_000;
  state.rateBuffer = state.rateBuffer.filter(t => t > cutoff);
  document.getElementById('stat-rate').textContent = state.rateBuffer.length;
}, 1000);

// chart aggregator: average per 5s window
const chartAggBuf = [];
setInterval(() => {
  if (!chartAggBuf.length) return;
  const avg = chartAggBuf.reduce((s, v) => s + v, 0) / chartAggBuf.length;
  pushChartSample(state.chartParam, avg, Date.now());
  chartAggBuf.length = 0;
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

function handle(msg) {
  if (msg.type === 'snapshot') {
    (msg.data.stations || []).forEach(s => {
      const rec = { location: s.location, lat: s.lat, lon: s.lon };
      upsertStation(rec);
      Object.entries(s.readings || {}).forEach(([p, v]) => {
        upsertStation({ location: s.location, lat: s.lat, lon: s.lon, parameter: p, value: v });
      });
    });
    (msg.data.alerts || []).forEach(addAlert);
    (msg.data.critical || []).forEach(addCritical);
    document.getElementById('stat-alerts').textContent = state.alerts.length;
    document.getElementById('stat-critical').textContent = state.critical.length;
    return;
  }
  if (msg.type === 'reading') {
    upsertStation(msg.data);
    addReading(msg.data);
    if (msg.data.parameter === state.chartParam && typeof msg.data.value === 'number') {
      chartAggBuf.push(msg.data.value);
    }
    return;
  }
  if (msg.type === 'enriched') {
    upsertStation({
      location: msg.data.location,
      lat: msg.data.lat, lon: msg.data.lon,
      parameter: msg.data.parameter, value: msg.data.avg_value,
    });
    return;
  }
  if (msg.type === 'alert')    { addAlert(msg.data); return; }
  if (msg.type === 'critical') { addCritical(msg.data); return; }
}

connect();
