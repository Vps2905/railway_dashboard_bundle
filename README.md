# Footfall Pilot v9 — 7SEMI ESP32-S3 EC200U

## Hardware: 7SEMI ESP32-S3 EC200U 4G v2.2

### ── CONFIRMED PIN MAP (board silkscreen, manual p.9) ──
| Board Label          | ESP32 GPIO | Function         |
|----------------------|-----------|-----------------|
| IO13 → 4G RX        | GPIO 13   | ESP32 TX → EC200U RX |
| IO12 → 4G TX        | GPIO 12   | ESP32 RX ← EC200U TX |
| IO8  → 4G RESET     | GPIO 8    | EC200U hard reset |
| IO16 → 4G ON/OFF    | GPIO 16   | EC200U PWRKEY    |

---

## 1. BACKEND — Railway Deploy

### First time
```bash
git clone https://github.com/Vps2905/railway_dashboard_bundle.git
cd railway_dashboard_bundle
npm install
npm start
```

### Railway Environment Variable (REQUIRED)
```
DEVICE_TOKEN = set_a_strong_shared_device_token
```
Set in Railway Dashboard → Your Service → Variables tab.

### Endpoints
| Method | Path         | Auth   | Description              |
|--------|-------------|--------|--------------------------|
| GET    | /health     | None   | Railway healthcheck      |
| GET    | /           | None   | Live dashboard           |
| POST   | /heartbeat  | Bearer | Device heartbeat         |
| POST   | /ingest     | Bearer | BLE/GNSS events batch    |
| POST   | /ota/check  | Bearer | OTA firmware check       |
| GET    | /api/dashboard | None | Dashboard data API    |
| GET    | /api/events | None   | Events export            |

---

## 2. FIRMWARE — Arduino IDE Flash

### Board Setup
1. Install ESP32 board package (Espressif) in Arduino IDE
2. Select: **Tools → Board → ESP32S3 Dev Module**
3. Set: **Tools → USB CDC on Boot → Enabled** ← REQUIRED per 7SEMI manual
4. Set: **Tools → CPU Frequency → 240MHz (WiFi)**
5. Required libraries (install via Library Manager):
   - `ESP32 BLE Arduino` (built-in with ESP32 package)
   - SPIFFS (built-in)
   - WiFi, WiFiClientSecure (built-in)

### Edit before flashing (pilot_firmware_v9/pilot_firmware_v9.ino)
```cpp
static const char* WIFI_SSID    = "YOUR_WIFI_SSID";
static const char* WIFI_PASS    = "YOUR_WIFI_PASSWORD";
static const char* DEVICE_TOKEN = "set_a_strong_shared_device_token";
static const char* BACKEND_HOST = "your-railway-service.up.railway.app";
```

### Flash
1. Connect board via ESP32-S3 USB port (right USB, not EC200U USB)
2. Upload sketch in Arduino IDE
3. **Press RST button on board after upload** (per 7SEMI manual)
4. Open Serial Monitor at 115200 baud

### Expected Serial Output
```
#############################################
  PILOT FIRMWARE v9
  7SEMI ESP32-S3 EC200U v2.2
  BLE + GNSS + WiFi → Railway Backend
#############################################

[SPIFFS] OK  spool=0B
[WIFI] connecting SSID=YOUR_WIFI_SSID
[WIFI] IP=192.168.x.x  RSSI=-55
[DNS] grateful-vibrancy... → 34.x.x.x ok=1
[MODEM] already ON   (or: PWRKEY pulse done...)
[GNSS]  OK
[BLE] ready
[READY] running  backend=...  device=iot_sn_001
```

---

## 3. TROUBLESHOOTING

| Symptom | Fix |
|---------|-----|
| `[MODEM] FAILED` | Check SIM inserted; try RST button; check power |
| `[GNSS] no fix yet` | Move outdoors, wait 2–5 min cold start |
| `[TLS] connect failed` | Events spool to flash, auto-retry. Check BACKEND_HOST |
| DNS fails | Set `TEMP_BACKEND_IP` in firmware with resolved IP |
| No events on dashboard | Check token matches Railway env var |
| Modem not responding | Increase boot delay or try `modemHwReset()` |

---

## 4. Architecture

```
7SEMI Board
├── ESP32-S3
│   ├── BLE passive scan (1s) → device table (512 entries, LRU)
│   ├── Presence tick (1s) → event queue (256 entries)
│   ├── Dwell sweep (1s) → exposure_exit events (min 10s dwell)
│   ├── SPIFFS spool (256KB) → survives WiFi outage
│   └── Upload burst (5s, batch 8) → HTTPS POST /ingest
│
├── EC200U modem (UART1, GPIO12/13)
│   ├── AT+QGPS=1 → GPS/GNSS enable
│   ├── AT+QGPSLOC=2 → lat/lon/time (5s poll)
│   └── PWRKEY = GPIO16 (LOW pulse = power toggle)
│
└── Railway Backend
    ├── POST /ingest → in-memory ring (20k events)
    ├── POST /heartbeat → device status table
    ├── GET /api/dashboard → KPIs + charts
    └── Socket.IO → real-time live feed
```
