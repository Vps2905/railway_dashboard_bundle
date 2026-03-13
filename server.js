// ============================================================================
// PILOT BACKEND v9 — server.js
// 7SEMI ESP32-S3 EC200U  |  BLE + GNSS Footfall Platform
// Deploy: Railway  |  Dashboard: GET /
// ============================================================================

const express    = require("express");
const http       = require("http");
const path       = require("path");
const { Server } = require("socket.io");

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, { cors: { origin: "*", methods: ["GET","POST"] } });

app.use(express.json({ limit: "1mb" }));
app.use(express.static(path.join(__dirname, "public")));

// ── CONFIG ────────────────────────────────────────────────────────────────────
const DEVICE_TOKEN = process.env.DEVICE_TOKEN || "my_test_token_123";
const PORT         = process.env.PORT          || 3000;
const MAX_EVENTS   = 20000;
const MASKED_TOKEN = DEVICE_TOKEN.length > 8
  ? `${DEVICE_TOKEN.slice(0, 4)}...${DEVICE_TOKEN.slice(-4)}`
  : "set-via-env";

// ── IN-MEMORY STORE ───────────────────────────────────────────────────────────
const store = {
  events:     [],
  heartbeats: {},
  stats: {
    total_ingested:   0,
    total_presence:   0,
    total_exits:      0,
    total_heartbeats: 0,
    auth_failures:    0,
    server_start:     new Date().toISOString(),
    last_ingest_at:   null,
  }
};

function pushEvent(ev) {
  store.events.push(ev);
  if (store.events.length > MAX_EVENTS) store.events.shift();
  store.stats.total_ingested++;
  if (ev.event_type === "presence")      store.stats.total_presence++;
  if (ev.event_type === "exposure_exit") store.stats.total_exits++;
  store.stats.last_ingest_at = new Date().toISOString();
}

// ── AUTH ──────────────────────────────────────────────────────────────────────
function auth(req, res, next) {
  const hdr   = req.headers["authorization"] || "";
  const token = hdr.replace(/^Bearer\s+/i, "").trim();
  if (token !== DEVICE_TOKEN) {
    store.stats.auth_failures++;
    console.warn(`[AUTH] FAIL  ip=${req.ip}  token="${token.slice(0,20)}..."`);
    return res.status(401).json({ error: "Unauthorized" });
  }
  next();
}

// ── DEVICE API ────────────────────────────────────────────────────────────────

// Health — no auth, Railway healthcheck
app.get("/health", (req, res) => {
  res.json({
    status:  "ok",
    uptime:  Math.floor(process.uptime()),
    events:  store.events.length,
    devices: Object.keys(store.heartbeats).length,
    ts:      new Date().toISOString(),
    version: "9.0.0",
  });
});

// Heartbeat
app.post("/heartbeat", auth, (req, res) => {
  const b  = req.body || {};
  const id = b.device_id || "unknown";
  store.heartbeats[id] = { ...b, _received_at: new Date().toISOString(), _ip: req.ip };
  store.stats.total_heartbeats++;

  console.log(
    `[HB]  dev=${id}  up=${b.uptime_sec}s  gps=${b.gps_fix}` +
    `  q=${b.queue_depth}  spool=${b.spool_bytes}B` +
    `  wifi=${b.wifi_rssi}dBm  modem=${b.modem_ready}  gnss=${b.gnss_ready}`
  );

  io.emit("heartbeat", store.heartbeats[id]);
  res.status(200).json({ ok: true });
});

// Ingest events
app.post("/ingest", auth, (req, res) => {
  const b = req.body || {};
  let events = [];
  if (Array.isArray(b.events)) events = b.events;
  else if (b.event_type)        events = [b];
  else return res.status(400).json({ error: "Expected {events:[...]} or single event" });

  if (!events.length) return res.status(400).json({ error: "Empty events array" });

  let accepted = 0;
  for (const ev of events) {
    if (!ev.event_type || !ev.device_id) {
      console.warn("[INGEST] skip malformed:", JSON.stringify(ev).slice(0,80));
      continue;
    }
    const enriched = { ...ev, _received_at: new Date().toISOString() };
    pushEvent(enriched);
    io.emit("event", enriched);
    accepted++;

    const gps = ev.gps_fix
      ? `${Number(ev.lat).toFixed(5)},${Number(ev.lon).toFixed(5)}`
      : "no-fix";
    console.log(
      `[EV]  type=${ev.event_type}  dev=${ev.device_id}` +
      `  rssi=${ev.rssi}  dwell=${ev.dwell_time_sec}s  gps=${gps}  sig=${ev.signal_source}`
    );
  }
  res.status(201).json({ accepted, total: store.events.length });
});

// OTA check
app.post("/ota/check", auth, (req, res) => {
  const b = req.body || {};
  console.log(`[OTA]  dev=${b.device_id}  fw=${b.fw_version}`);
  res.json({ update_available: false, current_fw: "9.0.0-field-test", message: "No update" });
});

// ── DASHBOARD API (no auth, browser) ─────────────────────────────────────────
app.get("/api/dashboard", (req, res) => {
  const now = Date.now();
  const H24 = 86400000;
  const H1  = 3600000;

  const recent24 = store.events.filter(e => {
    const t = e.timestamp_epoch
      ? e.timestamp_epoch * 1000
      : Date.parse(e._received_at || 0);
    return (now - t) < H24;
  });

  // Unique MACs
  const uniqueMacs = new Set(recent24.map(e => e.mac_hash).filter(Boolean));

  // Active in last 30s
  const activeSet = new Set(
    store.events
      .filter(e => e._received_at && (now - Date.parse(e._received_at)) < 30000)
      .map(e => e.mac_hash)
  );

  // Avg RSSI (active)
  const activeRssis = store.events
    .filter(e => e._received_at && (now - Date.parse(e._received_at)) < 30000 && e.rssi)
    .map(e => Number(e.rssi));
  const avgRssi = activeRssis.length
    ? Math.round(activeRssis.reduce((a,b) => a+b, 0) / activeRssis.length)
    : null;

  // GPS centroid
  const fixes = recent24.filter(e => e.gps_fix && e.lat && e.lon);
  let centLat = null, centLon = null;
  if (fixes.length) {
    centLat = (fixes.reduce((s,e) => s+Number(e.lat),0) / fixes.length).toFixed(7);
    centLon = (fixes.reduce((s,e) => s+Number(e.lon),0) / fixes.length).toFixed(7);
  }

  // Avg dwell (exits)
  const exitEvents = recent24.filter(e => e.event_type==="exposure_exit" && e.dwell_time_sec>0);
  const avgDwell = exitEvents.length
    ? Math.round(exitEvents.reduce((s,e) => s+e.dwell_time_sec,0) / exitEvents.length)
    : null;

  // Qualified exposure (dwell >= 10s)
  const qualified = exitEvents.filter(e => e.dwell_time_sec >= 10).length;

  // Hourly buckets (last 12h)
  const buckets = {};
  for (let i = 0; i < 12; i++) {
    const label = new Date(now - (11-i)*H1).toISOString().slice(11,13) + ":00";
    buckets[label] = { presence: 0, exits: 0 };
  }
  recent24.filter(e => (now - Date.parse(e._received_at||0)) < 12*H1).forEach(e => {
    const h = new Date(Date.parse(e._received_at||0)).toISOString().slice(11,13)+":00";
    if (buckets[h]) {
      if (e.event_type==="presence")      buckets[h].presence++;
      if (e.event_type==="exposure_exit") buckets[h].exits++;
    }
  });

  // RSSI histogram
  const rssiHist = { "-90 to -81":0, "-80 to -71":0, "-70 to -61":0, "-60 to -51":0, "-50+":0 };
  recent24.filter(e => e.rssi).forEach(e => {
    const r = Number(e.rssi);
    if (r<=-81)      rssiHist["-90 to -81"]++;
    else if (r<=-71) rssiHist["-80 to -71"]++;
    else if (r<=-61) rssiHist["-70 to -61"]++;
    else if (r<=-51) rssiHist["-60 to -51"]++;
    else             rssiHist["-50+"]++;
  });

  // Signal source breakdown
  const sigSources = {};
  recent24.forEach(e => {
    const s = e.signal_source || "unknown";
    sigSources[s] = (sigSources[s]||0) + 1;
  });

  // Recent 100 events for table
  const recentEvents = store.events.slice(-100).reverse().map(e => ({
    time:       e._received_at,
    event_type: e.event_type,
    device_id:  e.device_id,
    signal:     e.signal_source,
    rssi:       e.rssi,
    dwell:      e.dwell_time_sec,
    gps_fix:    e.gps_fix,
    lat:        e.lat,
    lon:        e.lon,
    mac_hash:   e.mac_hash ? e.mac_hash.slice(0,16)+"…" : "",
    fw:         e.fw_version,
    campaign_id:e.campaign_id,
    site_id:    e.site_id,
  }));

  res.json({
    stats:          store.stats,
    unique_devices: uniqueMacs.size,
    active_devices: activeSet.size,
    presence_24h:   recent24.filter(e=>e.event_type==="presence").length,
    exits_24h:      exitEvents.length,
    qualified_24h:  qualified,
    avg_rssi:       avgRssi,
    heartbeats:     Object.values(store.heartbeats),
    geofence:       { lat: centLat, lon: centLon, avg_dwell: avgDwell, fix_count: fixes.length },
    hourly_buckets: buckets,
    rssi_histogram: rssiHist,
    signal_sources: sigSources,
    recent_events:  recentEvents,
  });
});

// Events export
app.get("/api/events", (req, res) => {
  const limit  = Math.min(parseInt(req.query.limit  || "500"), 5000);
  const offset = parseInt(req.query.offset || "0");
  const type   = req.query.type;
  const filtered = type ? store.events.filter(e => e.event_type===type) : store.events;
  res.json({ total: filtered.length, offset, limit, events: filtered.slice(offset, offset+limit) });
});

// Root → dashboard
app.get("/", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));

// ── SOCKET.IO ─────────────────────────────────────────────────────────────────
io.on("connection", socket => {
  console.log(`[WS] connected ${socket.id}`);
  socket.emit("snapshot", {
    events:     store.events.slice(-100),
    heartbeats: Object.values(store.heartbeats),
    stats:      store.stats,
  });
  socket.on("disconnect", () => console.log(`[WS] disconnected ${socket.id}`));
});

// ── START ─────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n╔══════════════════════════════════════════════╗`);
  console.log(`║   PILOT BACKEND v9  —  7SEMI ESP32-S3        ║`);
  console.log(`║   Port : ${String(PORT).padEnd(36)}║`);
  console.log(`║   Token: ${MASKED_TOKEN.padEnd(36)}║`);
  console.log(`╚══════════════════════════════════════════════╝\n`);
});
