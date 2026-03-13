import os
import time
import json
import csv
import hashlib
import logging
import sqlite3
from io import StringIO
from math import radians, sin, cos, sqrt, atan2

from flask import Flask, request, jsonify, Response, send_from_directory, abort, g, render_template
from flask_socketio import SocketIO

DB_PATH = os.environ.get("DB_PATH", "footfall.db")
FIRMWARE_DIR = os.environ.get("FIRMWARE_DIR", "firmware_store")
MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH", str(2 * 1024 * 1024)))

GEOFENCE_LAT = float(os.environ.get("GEOFENCE_LAT", "17.43388"))
GEOFENCE_LON = float(os.environ.get("GEOFENCE_LON", "78.42669"))
GEOFENCE_RADIUS_M = float(os.environ.get("GEOFENCE_RADIUS_M", "300"))

ACTIVE_WINDOW_SEC = int(os.environ.get("ACTIVE_WINDOW_SEC", "30"))
EXPOSURE_DWELL_SEC = int(os.environ.get("EXPOSURE_DWELL_SEC", "10"))
EXPOSURE_RSSI_MIN = int(os.environ.get("EXPOSURE_RSSI_MIN", "-85"))

DEVICE_TOKEN = os.environ.get("DEVICE_TOKEN", "my_test_token_123")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "vps_2905")

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("footfall-server")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "change-me")
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

os.makedirs(FIRMWARE_DIR, exist_ok=True)


def get_db():
    if "db_conn" not in g:
        con = sqlite3.connect(DB_PATH, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("PRAGMA busy_timeout=5000")
        g.db_conn = con
    return g.db_conn


@app.teardown_appcontext
def close_db(_exc):
    con = g.pop("db_conn", None)
    if con is not None:
        con.close()


def parse_limit(value, default=200, minimum=1, maximum=100000):
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, n))


def auth_ok(req):
    auth = req.headers.get("Authorization", "")
    return auth.startswith("Bearer ") and auth.split(" ", 1)[1].strip() == DEVICE_TOKEN


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371000.0
    p1 = radians(lat1)
    p2 = radians(lat2)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(p1) * cos(p2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return r * c


def geofence_distance(lat, lon):
    if lat is None or lon is None:
        return None
    try:
        return haversine_m(float(lat), float(lon), GEOFENCE_LAT, GEOFENCE_LON)
    except (TypeError, ValueError):
        return None


def inside_geofence(lat, lon):
    d = geofence_distance(lat, lon)
    return d is not None and d <= GEOFENCE_RADIUS_M


def qualifies_exposure(ev):
    gps_fix = bool(ev.get("gps_fix"))
    dwell = ev.get("dwell_time_sec") or 0
    rssi = ev.get("rssi")
    lat = ev.get("lat")
    lon = ev.get("lon")

    if not gps_fix:
        return False
    if not inside_geofence(lat, lon):
        return False
    if dwell < EXPOSURE_DWELL_SEC:
        return False
    if rssi is None:
        return False
    try:
        return int(rssi) >= EXPOSURE_RSSI_MIN
    except (TypeError, ValueError):
        return False


def normalize_event(ev):
    gps_fix = bool(ev.get("gps_fix", False))
    lat = ev.get("lat")
    lon = ev.get("lon")
    distance = geofence_distance(lat, lon)
    inside = inside_geofence(lat, lon)
    qualified = qualifies_exposure(ev)

    return {
        "event_id": ev.get("event_id"),
        "schema_version": ev.get("schema_version"),
        "event_type": ev.get("event_type", "unknown"),
        "timestamp": ev.get("timestamp_epoch"),
        "timestamp_utc": ev.get("timestamp_utc"),
        "session_start_epoch": ev.get("session_start_epoch"),
        "session_end_epoch": ev.get("session_end_epoch"),
        "session_start_utc": ev.get("session_start_utc"),
        "session_end_utc": ev.get("session_end_utc"),
        "mac_hash": ev.get("mac_hash"),
        "signal_source": ev.get("signal_source"),
        "rssi": ev.get("rssi"),
        "dwell_time_sec": ev.get("dwell_time_sec"),
        "gps_fix": 1 if gps_fix else 0,
        "lat": lat,
        "lon": lon,
        "distance_to_geofence_m": distance,
        "inside_geofence": 1 if inside else 0,
        "qualified_exposure": 1 if qualified else 0,
        "device_id": ev.get("device_id"),
        "asset_id": ev.get("asset_id"),
        "asset_type": ev.get("asset_type"),
        "site_id": ev.get("site_id"),
        "creative_id": ev.get("creative_id"),
        "campaign_id": str(ev.get("campaign_id")) if ev.get("campaign_id") is not None else None,
        "activation_name": ev.get("activation_name"),
        "uplink_type": ev.get("uplink_type"),
        "fw_version": ev.get("fw_version"),
    }


def version_tuple(v):
    if not v:
        return (0,)
    parts = []
    for item in str(v).split("."):
        try:
            parts.append(int(item))
        except ValueError:
            parts.append(0)
    return tuple(parts)


def pick_release_for_device(cur, device_id, current_fw, channel="stable"):
    cur.execute(
        """
        SELECT *
        FROM ota_releases
        WHERE active = 1 AND channel = ?
        ORDER BY created_ts DESC
        """,
        (channel,),
    )
    rows = cur.fetchall()
    current_v = version_tuple(current_fw)
    bucket = int(hashlib.sha256(device_id.encode()).hexdigest(), 16) % 100

    for row in rows:
        target_v = version_tuple(row["version"])
        if target_v <= current_v:
            continue
        min_fw = row["min_fw_version"]
        if min_fw and current_v < version_tuple(min_fw):
            continue
        rollout_percent = int(row["rollout_percent"] or 100)
        if bucket >= rollout_percent:
            continue
        return row
    return None


def init_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        schema_version TEXT,
        event_type TEXT,
        received_ts INTEGER NOT NULL,
        timestamp INTEGER,
        timestamp_utc TEXT,
        session_start_epoch INTEGER,
        session_end_epoch INTEGER,
        session_start_utc TEXT,
        session_end_utc TEXT,
        mac_hash TEXT,
        signal_source TEXT,
        rssi INTEGER,
        dwell_time_sec INTEGER,
        gps_fix INTEGER,
        lat REAL,
        lon REAL,
        distance_to_geofence_m REAL,
        inside_geofence INTEGER,
        qualified_exposure INTEGER,
        device_id TEXT,
        asset_id TEXT,
        asset_type TEXT,
        site_id TEXT,
        creative_id TEXT,
        campaign_id TEXT,
        activation_name TEXT,
        uplink_type TEXT,
        fw_version TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT UNIQUE,
        site_id TEXT,
        asset_id TEXT,
        asset_type TEXT,
        fw_version TEXT,
        assigned_fw_version TEXT,
        auth_token TEXT,
        uplink_type TEXT,
        deployment_status TEXT,
        last_seen INTEGER,
        last_heartbeat_ts INTEGER,
        health_status TEXT,
        gps_fix INTEGER,
        lat REAL,
        lon REAL,
        queue_depth INTEGER,
        spool_bytes INTEGER,
        dropped_presence INTEGER,
        dropped_exit INTEGER,
        upload_failures INTEGER,
        wifi_status INTEGER,
        modem_ready INTEGER,
        ota_channel TEXT DEFAULT 'stable',
        ota_status TEXT,
        ota_last_checked INTEGER,
        ota_last_result TEXT,
        ota_last_target_version TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ota_releases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        version TEXT NOT NULL,
        channel TEXT NOT NULL DEFAULT 'stable',
        min_fw_version TEXT,
        binary_filename TEXT NOT NULL,
        binary_sha256 TEXT NOT NULL,
        binary_size INTEGER NOT NULL,
        notes TEXT,
        rollout_percent INTEGER NOT NULL DEFAULT 100,
        force_update INTEGER NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1,
        created_ts INTEGER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ota_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_id TEXT NOT NULL,
        current_fw_version TEXT,
        target_fw_version TEXT,
        status TEXT NOT NULL,
        message TEXT,
        reported_ts INTEGER NOT NULL
    )
    """)

    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id ON events(event_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_received_ts ON events(received_ts)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_mac_hash ON events(mac_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_device_id ON events(device_id)")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_devices_device_id ON devices(device_id)")

    con.commit()
    con.close()


def upsert_device_from_event(cur, norm, now_ts):
    if not norm.get("device_id"):
        return
    cur.execute(
        """
        INSERT INTO devices (
            device_id, site_id, asset_id, asset_type, fw_version, assigned_fw_version,
            auth_token, uplink_type, deployment_status, last_seen, health_status,
            gps_fix, lat, lon
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(device_id) DO UPDATE SET
            site_id=excluded.site_id,
            asset_id=excluded.asset_id,
            asset_type=excluded.asset_type,
            fw_version=excluded.fw_version,
            uplink_type=excluded.uplink_type,
            last_seen=excluded.last_seen,
            gps_fix=excluded.gps_fix,
            lat=excluded.lat,
            lon=excluded.lon
        """,
        (
            norm["device_id"], norm["site_id"], norm["asset_id"], norm["asset_type"],
            norm["fw_version"], norm["fw_version"], DEVICE_TOKEN, norm["uplink_type"],
            "testing", now_ts, "online", norm["gps_fix"], norm["lat"], norm["lon"],
        ),
    )


def compute_stats():
    cur = get_db().cursor()
    now = int(time.time())
    since_24h = now - 24 * 3600

    cur.execute("SELECT COUNT(DISTINCT mac_hash) AS n FROM events WHERE mac_hash IS NOT NULL")
    total_unique = cur.fetchone()["n"] or 0

    cur.execute("""
      SELECT mac_hash, MAX(received_ts) AS last_seen, AVG(rssi) AS avg_rssi
      FROM events
      WHERE mac_hash IS NOT NULL
      GROUP BY mac_hash
      HAVING (? - last_seen) <= ?
    """, (now, ACTIVE_WINDOW_SEC))
    active_rows = cur.fetchall()
    active_devices = len(active_rows)
    vals = [r["avg_rssi"] for r in active_rows if r["avg_rssi"] is not None]
    avg_rssi = (sum(vals) / len(vals)) if vals else 0.0

    cur.execute("""
      SELECT AVG(dwell_time_sec) AS avg_dwell
      FROM (
        SELECT dwell_time_sec
        FROM events
        WHERE dwell_time_sec IS NOT NULL
        ORDER BY id DESC
        LIMIT 200
      )
    """)
    avg_dwell = cur.fetchone()["avg_dwell"] or 0.0

    cur.execute("SELECT COUNT(*) AS n FROM events WHERE received_ts >= ? AND event_type = 'presence'", (since_24h,))
    presence_count = cur.fetchone()["n"] or 0

    cur.execute("SELECT COUNT(*) AS n FROM events WHERE received_ts >= ? AND event_type = 'exposure_exit'", (since_24h,))
    exposure_exit_count = cur.fetchone()["n"] or 0

    cur.execute("SELECT COUNT(*) AS n FROM events WHERE received_ts >= ? AND qualified_exposure = 1", (since_24h,))
    exposure_count = cur.fetchone()["n"] or 0

    return {
        "geofence": {"lat": GEOFENCE_LAT, "lon": GEOFENCE_LON, "radius_m": GEOFENCE_RADIUS_M},
        "total_unique": total_unique,
        "active_devices": active_devices,
        "avg_rssi": float(avg_rssi),
        "avg_dwell": float(avg_dwell),
        "presence_count": presence_count,
        "exposure_exit_count": exposure_exit_count,
        "exposure_count": exposure_count,
    }


@app.get("/")
def home():
    return render_template("index.html")


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "ble-gnss-command-center", "ts": int(time.time())})


@app.get("/api/stats")
def api_stats():
    return jsonify(compute_stats())


@app.get("/api/events")
def api_events():
    limit = parse_limit(request.args.get("limit"), default=200, maximum=5000)
    cur = get_db().cursor()
    cur.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))
    return jsonify([dict(r) for r in cur.fetchall()])


@app.get("/api/devices")
def api_devices():
    limit = parse_limit(request.args.get("limit"), default=500, maximum=5000)
    cur = get_db().cursor()
    cur.execute("SELECT * FROM devices ORDER BY COALESCE(last_heartbeat_ts, last_seen, 0) DESC LIMIT ?", (limit,))
    return jsonify([dict(r) for r in cur.fetchall()])


@app.post("/ingest")
def ingest():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    if isinstance(payload, dict) and "events" in payload:
        events = payload.get("events") or []
    elif isinstance(payload, dict):
        events = [payload]
    else:
        return jsonify({"ok": False, "error": "invalid payload"}), 400

    received_ts = int(time.time())
    con = get_db()
    cur = con.cursor()

    inserted = 0
    duplicates = 0
    skipped = 0
    last_event = None

    for ev in events:
        if not isinstance(ev, dict):
            skipped += 1
            continue

        norm = normalize_event(ev)
        last_event = norm

        if not norm["event_id"] or not norm["mac_hash"] or not norm["device_id"]:
            skipped += 1
            continue

        try:
            cur.execute("""
              INSERT INTO events (
                event_id, schema_version, event_type,
                received_ts, timestamp, timestamp_utc,
                session_start_epoch, session_end_epoch, session_start_utc, session_end_utc,
                mac_hash, signal_source, rssi, dwell_time_sec,
                gps_fix, lat, lon,
                distance_to_geofence_m, inside_geofence, qualified_exposure,
                device_id, asset_id, asset_type, site_id, creative_id,
                campaign_id, activation_name, uplink_type, fw_version
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                norm["event_id"], norm["schema_version"], norm["event_type"],
                received_ts, norm["timestamp"], norm["timestamp_utc"],
                norm["session_start_epoch"], norm["session_end_epoch"], norm["session_start_utc"], norm["session_end_utc"],
                norm["mac_hash"], norm["signal_source"], norm["rssi"], norm["dwell_time_sec"],
                norm["gps_fix"], norm["lat"], norm["lon"],
                norm["distance_to_geofence_m"], norm["inside_geofence"], norm["qualified_exposure"],
                norm["device_id"], norm["asset_id"], norm["asset_type"], norm["site_id"], norm["creative_id"],
                norm["campaign_id"], norm["activation_name"], norm["uplink_type"], norm["fw_version"],
            ))
            inserted += 1
            upsert_device_from_event(cur, norm, received_ts)
        except sqlite3.IntegrityError:
            duplicates += 1

    con.commit()

    socketio.emit("ingest", {
        "count": inserted,
        "duplicates": duplicates,
        "skipped": skipped,
        "last": last_event,
        "stats": compute_stats(),
    })
    return jsonify({"ok": True, "inserted": inserted, "duplicates": duplicates, "skipped": skipped})


@app.post("/heartbeat")
def heartbeat():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    device_id = payload.get("device_id")
    if not device_id:
        return jsonify({"ok": False, "error": "device_id required"}), 400

    now_ts = int(time.time())
    con = get_db()
    cur = con.cursor()

    cur.execute("""
      INSERT INTO devices (
        device_id, site_id, asset_id, asset_type, fw_version, assigned_fw_version,
        auth_token, uplink_type, deployment_status, last_seen, last_heartbeat_ts,
        health_status, gps_fix, lat, lon, queue_depth, spool_bytes,
        dropped_presence, dropped_exit, upload_failures, wifi_status, modem_ready, ota_channel
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(device_id) DO UPDATE SET
        site_id=excluded.site_id,
        asset_id=excluded.asset_id,
        asset_type=excluded.asset_type,
        fw_version=excluded.fw_version,
        uplink_type=excluded.uplink_type,
        last_seen=excluded.last_seen,
        last_heartbeat_ts=excluded.last_heartbeat_ts,
        health_status=excluded.health_status,
        gps_fix=excluded.gps_fix,
        lat=excluded.lat,
        lon=excluded.lon,
        queue_depth=excluded.queue_depth,
        spool_bytes=excluded.spool_bytes,
        dropped_presence=excluded.dropped_presence,
        dropped_exit=excluded.dropped_exit,
        upload_failures=excluded.upload_failures,
        wifi_status=excluded.wifi_status,
        modem_ready=excluded.modem_ready,
        ota_channel=excluded.ota_channel
    """, (
        device_id, payload.get("site_id"), payload.get("asset_id"), payload.get("asset_type"),
        payload.get("fw_version"), payload.get("fw_version"), DEVICE_TOKEN, payload.get("uplink_type"),
        "testing", now_ts, now_ts, "online",
        1 if payload.get("gps_fix") else 0, payload.get("lat"), payload.get("lon"),
        payload.get("queue_depth"), payload.get("spool_bytes"), payload.get("dropped_presence"),
        payload.get("dropped_exit"), payload.get("upload_failures"), payload.get("wifi_status"),
        1 if payload.get("modem_ready") else 0, payload.get("ota_channel", "stable"),
    ))

    con.commit()
    socketio.emit("heartbeat", {"device_id": device_id, "ts": now_ts, "stats": compute_stats()})
    return jsonify({"ok": True, "device_id": device_id})


@app.post("/ota/check")
def ota_check():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    device_id = payload.get("device_id")
    current_fw = payload.get("fw_version")
    channel = payload.get("ota_channel", "stable")

    if not device_id or not current_fw:
        return jsonify({"ok": False, "error": "device_id and fw_version required"}), 400

    now_ts = int(time.time())
    con = get_db()
    cur = con.cursor()

    cur.execute("""
      INSERT INTO devices (device_id, fw_version, assigned_fw_version, auth_token, ota_channel, ota_last_checked)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(device_id) DO UPDATE SET
        fw_version=excluded.fw_version,
        ota_channel=excluded.ota_channel,
        ota_last_checked=excluded.ota_last_checked
    """, (device_id, current_fw, current_fw, DEVICE_TOKEN, channel, now_ts))

    rel = pick_release_for_device(cur, device_id, current_fw, channel)

    if rel is None:
        cur.execute(
            "UPDATE devices SET ota_last_result = ?, ota_last_target_version = ? WHERE device_id = ?",
            ("up_to_date", current_fw, device_id),
        )
        con.commit()
        return jsonify({"ok": True, "update_available": False, "device_id": device_id, "current_version": current_fw})

    cur.execute(
        "UPDATE devices SET assigned_fw_version = ?, ota_last_result = ?, ota_last_target_version = ? WHERE device_id = ?",
        (rel["version"], "update_available", rel["version"], device_id),
    )
    con.commit()

    return jsonify({
        "ok": True,
        "update_available": True,
        "device_id": device_id,
        "current_version": current_fw,
        "target_version": rel["version"],
        "force_update": bool(rel["force_update"]),
        "binary_sha256": rel["binary_sha256"],
        "binary_size": rel["binary_size"],
        "notes": rel["notes"] or "",
        "download_url": f"/firmware/{rel['binary_filename']}",
    })


@app.post("/ota/report")
def ota_report():
    if not auth_ok(request):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(force=True, silent=True)
    if payload is None:
        return jsonify({"ok": False, "error": "invalid json"}), 400

    device_id = payload.get("device_id")
    status = payload.get("status")
    if not device_id or not status:
        return jsonify({"ok": False, "error": "device_id and status required"}), 400

    con = get_db()
    cur = con.cursor()
    now_ts = int(time.time())

    cur.execute("""
      INSERT INTO ota_reports (device_id, current_fw_version, target_fw_version, status, message, reported_ts)
      VALUES (?, ?, ?, ?, ?, ?)
    """, (
        device_id,
        payload.get("current_fw_version"),
        payload.get("target_fw_version"),
        status,
        payload.get("message", ""),
        now_ts,
    ))

    cur.execute("""
      UPDATE devices
      SET ota_status = ?, ota_last_result = ?, assigned_fw_version = COALESCE(?, assigned_fw_version)
      WHERE device_id = ?
    """, (status, status, payload.get("target_fw_version"), device_id))

    con.commit()
    return jsonify({"ok": True})


@app.get("/firmware/<path:filename>")
def firmware_download(filename):
    full_path = os.path.abspath(os.path.join(FIRMWARE_DIR, filename))
    firmware_root = os.path.abspath(FIRMWARE_DIR)
    if not full_path.startswith(firmware_root):
        abort(404)
    if not os.path.exists(full_path):
        abort(404)
    return send_from_directory(FIRMWARE_DIR, filename, as_attachment=True)


init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    socketio.run(app, host="0.0.0.0", port=port)
