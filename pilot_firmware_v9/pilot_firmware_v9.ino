// ============================================================================
// PILOT FIRMWARE v9 — 7SEMI ESP32-S3 EC200U 4G Board
// Hardware: 7SEMI v2.2  |  Manual confirmed pins below
// BLE Footfall + GNSS + WiFi → Railway HTTPS Backend
// ============================================================================
//
// ── PIN MAP (from 7SEMI v2.2 board labels + manual p.15) ──────────────────
//   EC200U TX  → ESP32 RX  = GPIO 13   (board label: IO13 → 4G RX)
//   EC200U RX  ← ESP32 TX  = GPIO 12   (board label: IO12 → 4G TX)
//   EC200U KEY   PWRKEY     = GPIO 16   (board label: IO16 → 4G ON/OFF)
//   EC200U RST   RESET      = GPIO  8   (board label: IO8  → 4G RESET)
//   UART port  = Serial2 (UART1)
//   Baud rate  = 115200
//
// ── ARDUINO IDE SETTINGS ──────────────────────────────────────────────────
//   Board:          ESP32S3 Dev Module
//   USB CDC on Boot: Enabled   ← REQUIRED per 7SEMI manual p.16
//   CPU Frequency:  240 MHz (WiFi)
//   Flash Size:     4MB or 8MB (match your module)
//   Upload Speed:   921600
//   After upload → press RST button on board
// ============================================================================

#include <Arduino.h>
#include <cstring>
#include <cstdio>
#include <time.h>

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <SPIFFS.h>
#include <BLEDevice.h>
#include <BLEScan.h>
#include <BLEAdvertisedDevice.h>
#include "mbedtls/sha256.h"
#include "nvs_flash.h"
#include "esp_err.h"

// ============================================================================
// ★ FIELD CONFIG — EDIT BEFORE FLASHING ★
// ============================================================================
static const char* WIFI_SSID      = "YOUR_WIFI_SSID";
static const char* WIFI_PASS      = "YOUR_WIFI_PASSWORD";
static const char* DEVICE_TOKEN   = "my_test_token_123";

static const char* DEVICE_ID      = "iot_sn_001";
static const char* SITE_ID        = "site_001";
static const char* ASSET_ID       = "asset_001";
static const char* ASSET_TYPE     = "bike_mobile";
static const char* CREATIVE_ID    = "Creative_A";
static const char* ACTIVATION     = "Pilot_GT";
static const uint32_t CAMPAIGN_ID = 1023;
static const char* SCHEMA_VERSION = "1.0";
static const char* FW_VERSION     = "9.0.0-7semi-field";

// ============================================================================
// RAILWAY BACKEND
// ============================================================================
static const char* BACKEND_HOST = "your-railway-service.up.railway.app";
static const uint16_t BACKEND_PORT = 443;

// If DNS fails, put resolved IP here e.g. "34.120.55.10"
// Get it by: ping your-railway-service.up.railway.app
static const char* TEMP_BACKEND_IP = "";

static const char* PATH_HEARTBEAT = "/heartbeat";
static const char* PATH_INGEST    = "/ingest";

// ============================================================================
// 7SEMI BOARD HARDWARE PINS  (confirmed from board silkscreen v2.2)
// ============================================================================
static const int MODEM_RX_PIN  = 13;   // ESP32 RX ← EC200U TX  (IO13→4G RX)
static const int MODEM_TX_PIN  = 12;   // ESP32 TX → EC200U RX  (IO12→4G TX)
static const int MODEM_PWRKEY  = 16;   // 4G ON/OFF             (IO16→4G ON/OFF)
static const int MODEM_RST_PIN =  8;   // 4G RESET              (IO8 →4G RESET)
static const uint32_t MODEM_BAUD = 115200;

HardwareSerial Modem(1);  // UART1

// ============================================================================
// TIMING
// ============================================================================
static const uint32_t GNSS_POLL_MS            = 5000;
static const uint32_t GNSS_RETRY_MS           = 30000;
static const uint32_t HEARTBEAT_PERIOD_MS     = 60000;
static const uint32_t UPLOAD_PERIOD_MS        = 5000;
static const uint32_t WIFI_CONNECT_TIMEOUT_MS = 15000;
static const uint32_t TLS_TIMEOUT_MS          = 15000;
static const uint32_t STATUS_PRINT_MS         = 10000;

static const uint16_t MAX_DEVICES             = 512;
static const uint16_t EVENT_Q_LEN             = 256;
static const uint8_t  BATCH_MAX_EVENTS        = 8;

static const uint32_t EXIT_THRESHOLD_SEC      = 20;
static const uint32_t MIN_DWELL_SEC           = 10;
static const uint32_t PRESENCE_TICK_MS        = 1000;
static const uint32_t PRESENCE_RECENT_SEC     = 1;
static const uint16_t PRESENCE_MAX_PER_TICK   = 20;

static const uint32_t BACKOFF_MIN_MS          = 5000;
static const uint32_t BACKOFF_MAX_MS          = 60000;

static const char*  SPOOL_PATH                = "/spool.jsonl";
static const size_t SPOOL_MAX_BYTES           = 256 * 1024;

// ============================================================================
// DATA TYPES
// ============================================================================
struct DeviceEntry {
  bool     used;
  uint8_t  mac_hash[32];
  int8_t   last_rssi;
  uint64_t first_seen_up_s;
  uint64_t last_seen_up_s;
  uint64_t last_touch_up_s;
  char     signal_source[16];
};

struct FootfallEvent {
  char     schema_version[8];
  char     event_id[64];
  char     event_type[24];
  uint64_t timestamp_epoch;
  char     timestamp_utc[21];
  uint64_t session_start_epoch;
  uint64_t session_end_epoch;
  char     session_start_utc[21];
  char     session_end_utc[21];
  uint8_t  mac_hash[32];
  int8_t   rssi;
  uint32_t dwell_time_sec;
  bool     gps_fix;
  double   lat;
  double   lon;
  char     signal_source[16];
  char     device_id[32];
  char     site_id[32];
  char     asset_id[32];
  char     asset_type[24];
  char     creative_id[32];
  uint32_t campaign_id;
  char     activation_name[48];
  char     uplink_type[20];
  char     fw_version[24];
};

// ============================================================================
// GLOBALS
// ============================================================================
static portMUX_TYPE g_qMux   = portMUX_INITIALIZER_UNLOCKED;
static portMUX_TYPE g_tabMux = portMUX_INITIALIZER_UNLOCKED;

static DeviceEntry   g_tab[MAX_DEVICES];
static FootfallEvent g_q[EVENT_Q_LEN];
static FootfallEvent g_uploadBatch[BATCH_MAX_EVENTS];

static volatile uint16_t g_q_head = 0;
static volatile uint16_t g_q_tail = 0;

static BLEScan* pBLEScan   = nullptr;
static bool g_bleReady     = false;
static bool g_spiffs_ok    = false;
static bool g_wifiReady    = false;
static bool g_modemReady   = false;
static bool g_gnssReady    = false;
static bool g_backendDnsOk = false;

static volatile bool     g_fix        = false;
static volatile double   g_lat        = 0.0;
static volatile double   g_lon        = 0.0;
static volatile uint64_t g_lastUtc    = 0;
static volatile uint64_t g_lastUtcUpS = 0;

static uint8_t g_salt[16];

static uint32_t g_lastGnssMs       = 0;
static uint32_t g_lastGnssRetryMs  = 0;
static uint32_t g_lastBleKickMs    = 0;
static uint32_t g_lastSweepMs      = 0;
static uint32_t g_lastPresenceMs   = 0;
static uint32_t g_lastHeartbeatMs  = 0;
static uint32_t g_nextUploadMs     = 0;
static uint32_t g_lastStatusMs     = 0;
static uint32_t g_backoffMs        = BACKOFF_MIN_MS;

static uint32_t g_droppedPresence  = 0;
static uint32_t g_droppedExit      = 0;
static uint32_t g_uploadFailures   = 0;
static uint32_t g_totalPresence    = 0;
static uint32_t g_totalExit        = 0;
static uint32_t g_totalUploaded    = 0;

// ============================================================================
// HELPERS
// ============================================================================
static inline uint64_t up_s() { return (uint64_t)(millis() / 1000ULL); }

static void copyStr(char* dst, size_t sz, const char* src) {
  if (!dst || sz == 0) return;
  memset(dst, 0, sz);
  if (src) strncpy(dst, src, sz - 1);
}

static const char* wifiName(wl_status_t s) {
  switch (s) {
    case WL_CONNECTED:       return "CONNECTED";
    case WL_CONNECT_FAILED:  return "FAIL";
    case WL_NO_SSID_AVAIL:   return "NO_SSID";
    case WL_DISCONNECTED:    return "DISCONNECTED";
    default:                 return "OTHER";
  }
}

static uint64_t nowUtc() {
  if (g_lastUtc == 0) return 0;
  return g_lastUtc + (up_s() - g_lastUtcUpS);
}

static void epochToIso(uint64_t epoch, char out[21]) {
  if (epoch == 0) { out[0] = '\0'; return; }
  time_t tt = (time_t)epoch;
  struct tm t;
  gmtime_r(&tt, &t);
  snprintf(out, 21, "%04d-%02d-%02dT%02d:%02d:%02dZ",
           t.tm_year + 1900, t.tm_mon + 1, t.tm_mday,
           t.tm_hour, t.tm_min, t.tm_sec);
}

static void genEventId(char out[64]) {
  snprintf(out, 64, "evt_%s_%llu_%08x",
           DEVICE_ID, (unsigned long long)up_s(), (unsigned)esp_random());
}

static void sha256_2(uint8_t out[32],
                     const uint8_t* d1, size_t l1,
                     const uint8_t* d2, size_t l2) {
  mbedtls_sha256_context c;
  mbedtls_sha256_init(&c);
  mbedtls_sha256_starts(&c, 0);
  mbedtls_sha256_update(&c, d1, l1);
  mbedtls_sha256_update(&c, d2, l2);
  mbedtls_sha256_finish(&c, out);
  mbedtls_sha256_free(&c);
}

static void hashMac(uint8_t out[32], const uint8_t mac[6]) {
  sha256_2(out, mac, 6, g_salt, 16);
}

static void hashHex(const uint8_t h[32], char out[65]) {
  static const char* hx = "0123456789abcdef";
  for (int i = 0; i < 32; i++) {
    out[i*2]   = hx[(h[i]>>4)&0xF];
    out[i*2+1] = hx[h[i]&0xF];
  }
  out[64] = 0;
}

static uint64_t makeEpoch(int Y, int M, int D, int h, int m, int s) {
  static const uint16_t dpm[] = {0,31,59,90,120,151,181,212,243,273,304,334};
  auto leap = [](int y){ return ((y%4==0)&&(y%100!=0))||(y%400==0); };
  if (Y<1970||M<1||M>12||D<1||D>31) return 0;
  uint64_t days = 0;
  for (int y = 1970; y < Y; y++) days += leap(y) ? 366 : 365;
  days += dpm[M-1];
  if (M>2 && leap(Y)) days++;
  days += (D-1);
  return days*86400ULL + (uint64_t)h*3600 + (uint64_t)m*60 + s;
}

// ============================================================================
// WIFI
// ============================================================================
static void wifiInit() {
  if (g_wifiReady) return;
  WiFi.persistent(false);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  WiFi.mode(WIFI_STA);
  delay(300);
  g_wifiReady = true;
}

static bool wifiConnect() {
  wifiInit();
  if (WiFi.status() == WL_CONNECTED) return true;
  Serial.printf("[WIFI] connecting SSID=%s\n", WIFI_SSID);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  uint32_t t0 = millis();
  while (millis()-t0 < WIFI_CONNECT_TIMEOUT_MS) {
    wl_status_t st = WiFi.status();
    if (st == WL_CONNECTED) {
      Serial.printf("[WIFI] IP=%s  RSSI=%d\n",
                    WiFi.localIP().toString().c_str(), WiFi.RSSI());
      return true;
    }
    delay(250);
  }
  Serial.printf("[WIFI] timeout status=%s\n", wifiName(WiFi.status()));
  return false;
}

static bool resolveBackend(IPAddress& ip) {
  if (strlen(TEMP_BACKEND_IP) > 0 && ip.fromString(TEMP_BACKEND_IP)) {
    Serial.printf("[DNS] override IP=%s\n", TEMP_BACKEND_IP);
    g_backendDnsOk = true;
    return true;
  }
  bool ok = WiFi.hostByName(BACKEND_HOST, ip);
  g_backendDnsOk = ok && ip != IPAddress((uint32_t)0);
  Serial.printf("[DNS] %s → %s ok=%d\n",
                BACKEND_HOST, ip.toString().c_str(), g_backendDnsOk?1:0);
  return g_backendDnsOk;
}

// ============================================================================
// EC200U MODEM — 7SEMI v2.2 confirmed behaviour
// PWRKEY (GPIO16) = IO16→4G ON/OFF
// Per 7SEMI manual sample code: drive LOW to enable/trigger
// EC200U PWRKEY: active-LOW edge triggers power toggle
// ============================================================================
static void modemFlush() { while (Modem.available()) Modem.read(); }

static String modemRead(uint32_t ms) {
  String r;
  uint32_t t0 = millis();
  while (millis()-t0 < ms) {
    while (Modem.available()) r += (char)Modem.read();
    delay(5);
  }
  return r;
}

static String atCmd(const String& cmd, uint32_t ms = 2000, bool verbose = false) {
  modemFlush();
  Modem.print(cmd); Modem.print("\r\n");
  String r = modemRead(ms);
  if (verbose || r.indexOf("ERROR") >= 0) {
    Serial.printf("[AT] %s → %s\n", cmd.c_str(),
                  r.length() ? r.c_str() : "<empty>");
  }
  return r;
}

static bool atExpect(const String& cmd, const String& expect, uint32_t ms=3000) {
  return atCmd(cmd, ms, true).indexOf(expect) >= 0;
}

// 7SEMI PWRKEY: Pull GPIO16 LOW for ~1.2s to toggle power
static void modemPwrPulse() {
  pinMode(MODEM_PWRKEY, OUTPUT);
  digitalWrite(MODEM_PWRKEY, HIGH);
  delay(100);
  digitalWrite(MODEM_PWRKEY, LOW);   // Active low: trigger power on
  delay(1200);
  digitalWrite(MODEM_PWRKEY, HIGH);
  Serial.println("[MODEM] PWRKEY pulse done, waiting 5s boot...");
  delay(5000);
}

// Hardware reset via RST pin (GPIO8)
static void modemHwReset() {
  if (MODEM_RST_PIN < 0) return;
  pinMode(MODEM_RST_PIN, OUTPUT);
  digitalWrite(MODEM_RST_PIN, LOW);
  delay(200);
  digitalWrite(MODEM_RST_PIN, HIGH);
  Serial.println("[MODEM] HW reset done, waiting 3s...");
  delay(3000);
}

static bool modemInit() {
  Modem.end(); delay(100);
  Modem.begin(MODEM_BAUD, SERIAL_8N1, MODEM_RX_PIN, MODEM_TX_PIN);
  delay(500);

  // Try AT first — may already be running
  if (atExpect("AT", "OK", 1500)) {
    Serial.println("[MODEM] already ON");
    atCmd("ATE0", 1000, true);  // Echo off
    return true;
  }

  // PWRKEY pulse
  modemPwrPulse();

  for (int i = 1; i <= 10; i++) {
    Serial.printf("[MODEM] probe %d/10\n", i);
    if (atExpect("AT", "OK", 1500)) {
      atCmd("ATE0", 1000, true);
      Serial.println("[MODEM] OK after power pulse");
      return true;
    }
    delay(500);
  }

  // Try hardware reset as fallback
  Serial.println("[MODEM] trying HW reset...");
  modemHwReset();
  for (int i = 1; i <= 6; i++) {
    if (atExpect("AT", "OK", 2000)) {
      atCmd("ATE0", 1000, true);
      Serial.println("[MODEM] OK after HW reset");
      return true;
    }
    delay(500);
  }

  Serial.println("[MODEM] FAILED — check SIM, antenna, power");
  return false;
}

static bool gnssStart() {
  if (!atExpect("AT", "OK", 1500)) return false;

  // Check if GNSS already running
  if (atCmd("AT+QGPS?", 1500, true).indexOf("+QGPS: 1") >= 0) {
    Serial.println("[GNSS] already running");
    return true;
  }

  Serial.println("[GNSS] starting with AT+QGPS=1");
  if (atCmd("AT+QGPS=1", 4000, true).indexOf("OK") >= 0) {
    Serial.println("[GNSS] started OK");
    return true;
  }
  Serial.println("[GNSS] start FAILED");
  return false;
}

// 7SEMI manual uses AT+QGPSLOC=2  (format: UTC,lat,lon,accuracy,altitude,fix,...)
// Response example from manual p.19:
// +QGPSLOC: 065633.000,19.05496,73.01729,2.0,22.2,3,000.00,0.6,0.3,250225,11
//            ^UTC       ^lat     ^lon
static bool gnssPoll(bool& fix, double& lat, double& lon, uint64_t& epoch) {
  String r = atCmd("AT+QGPSLOC=2", 5000, false);

  int p = r.indexOf("+QGPSLOC:");
  if (p < 0) { fix = false; return true; }

  int colon = r.indexOf(':', p);
  if (colon < 0) { fix = false; return true; }

  String line = r.substring(colon + 1);
  line.trim();
  int eol = line.indexOf('\n');
  if (eol >= 0) line = line.substring(0, eol);
  line.trim();

  // Fields: UTC,lat,lon,accuracy,altitude,fixmode,cog,spkm,spkn,date,nsat
  int c[11];
  int pos = 0;
  c[0] = -1;
  int fc = 0;
  for (int i = 0; i < (int)line.length() && fc < 10; i++) {
    if (line[i] == ',') { c[fc++] = i; }
  }

  if (fc < 9) { fix = false; return true; }  // need at least 10 fields

  String utcStr  = line.substring(0, c[0]);          utcStr.trim();
  String latStr  = line.substring(c[0]+1, c[1]);     latStr.trim();
  String lonStr  = line.substring(c[1]+1, c[2]);     lonStr.trim();
  String dateStr = line.substring(c[8]+1, c[9] >= 0 ? c[9] : line.length()); dateStr.trim();
  // dateStr is DDMMYY format e.g. "250225" = 25 Feb 2025

  lat = latStr.toDouble();
  lon = lonStr.toDouble();

  if (lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0) {
    fix = false; return true;
  }

  if (utcStr.length() < 6 || dateStr.length() < 6) {
    fix = true; epoch = 0; return true;
  }

  // UTC = HHMMSS.sss, date = DDMMYY
  epoch = makeEpoch(
    dateStr.substring(4, 6).toInt() + 2000,
    dateStr.substring(2, 4).toInt(),
    dateStr.substring(0, 2).toInt(),
    utcStr.substring(0, 2).toInt(),
    utcStr.substring(2, 4).toInt(),
    utcStr.substring(4, 6).toInt()
  );

  fix = true;
  return true;
}

// ============================================================================
// QUEUE
// ============================================================================
static uint16_t qDepth() {
  portENTER_CRITICAL(&g_qMux);
  uint16_t h = g_q_head, t = g_q_tail;
  portEXIT_CRITICAL(&g_qMux);
  return (h >= t) ? (h-t) : (EVENT_Q_LEN-t+h);
}

static bool qPush(const FootfallEvent& ev) {
  portENTER_CRITICAL(&g_qMux);
  uint16_t next = (g_q_head+1) % EVENT_Q_LEN;
  if (next == g_q_tail) { portEXIT_CRITICAL(&g_qMux); return false; }
  g_q[g_q_head] = ev;
  g_q_head = next;
  portEXIT_CRITICAL(&g_qMux);
  return true;
}

static bool qPop(FootfallEvent& ev) {
  portENTER_CRITICAL(&g_qMux);
  if (g_q_tail == g_q_head) { portEXIT_CRITICAL(&g_qMux); return false; }
  ev = g_q[g_q_tail];
  g_q_tail = (g_q_tail+1) % EVENT_Q_LEN;
  portEXIT_CRITICAL(&g_qMux);
  return true;
}

// ============================================================================
// SPIFFS SPOOL
// ============================================================================
static size_t spoolSize() {
  if (!g_spiffs_ok) return 0;
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return 0;
  size_t s = f.size(); f.close(); return s;
}

static void spoolTrimHead() {
  if (!g_spiffs_ok) return;
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return;
  f.readStringUntil('\n');
  String rest = f.readString(); f.close();
  SPIFFS.remove(SPOOL_PATH);
  if (rest.length()) {
    File w = SPIFFS.open(SPOOL_PATH, FILE_WRITE);
    if (w) { w.print(rest); w.close(); }
  }
}

static void spoolAppend(const String& line) {
  if (!g_spiffs_ok || !line.length()) return;
  while (spoolSize() > SPOOL_MAX_BYTES) spoolTrimHead();
  File f = SPIFFS.open(SPOOL_PATH, FILE_APPEND);
  if (f) { f.print(line); f.print("\n"); f.close(); }
}

static bool spoolPop(String& out) {
  if (!g_spiffs_ok) return false;
  File f = SPIFFS.open(SPOOL_PATH, FILE_READ);
  if (!f) return false;
  out = f.readStringUntil('\n'); out.trim();
  String rest = f.readString(); f.close();
  SPIFFS.remove(SPOOL_PATH);
  if (rest.length()) {
    File w = SPIFFS.open(SPOOL_PATH, FILE_WRITE);
    if (w) { w.print(rest); w.close(); }
  }
  return out.length() > 0;
}

// ============================================================================
// BLE
// ============================================================================
static int tabFind(const uint8_t h[32]) {
  for (int i = 0; i < MAX_DEVICES; i++)
    if (g_tab[i].used && memcmp(g_tab[i].mac_hash, h, 32)==0) return i;
  return -1;
}

static int tabAlloc() {
  for (int i = 0; i < MAX_DEVICES; i++) if (!g_tab[i].used) return i;
  int lru = 0;
  uint64_t lt = g_tab[0].last_touch_up_s;
  for (int i = 1; i < MAX_DEVICES; i++)
    if (g_tab[i].last_touch_up_s < lt) { lt = g_tab[i].last_touch_up_s; lru = i; }
  return lru;
}

static void observeMac(const uint8_t mac[6], int8_t rssi, const char* src) {
  uint8_t h[32]; hashMac(h, mac);
  uint64_t t = up_s();
  portENTER_CRITICAL(&g_tabMux);
  int idx = tabFind(h);
  if (idx < 0) idx = tabAlloc();
  DeviceEntry& e = g_tab[idx];
  if (!e.used || memcmp(e.mac_hash, h, 32)) {
    memset(&e, 0, sizeof(e));
    e.used = true;
    memcpy(e.mac_hash, h, 32);
    e.first_seen_up_s = t;
  }
  e.last_seen_up_s  = t;
  e.last_touch_up_s = t;
  e.last_rssi       = rssi;
  copyStr(e.signal_source, 16, src);
  portEXIT_CRITICAL(&g_tabMux);
}

class BLECB : public BLEAdvertisedDeviceCallbacks {
  void onResult(BLEAdvertisedDevice dev) override {
    String s = dev.getAddress().toString();
    int v[6]; uint8_t mac[6];
    if (sscanf(s.c_str(), "%x:%x:%x:%x:%x:%x",
               &v[0],&v[1],&v[2],&v[3],&v[4],&v[5]) == 6) {
      for (int i=0;i<6;i++) mac[i]=(uint8_t)v[i];
      observeMac(mac, (int8_t)dev.getRSSI(), "ble");
    }
  }
};
static BLECB g_blecb;

static bool bleInit() {
  Serial.println("[BLE] init...");
  BLEDevice::init("");
  pBLEScan = BLEDevice::getScan();
  if (!pBLEScan) { Serial.println("[BLE] FAIL"); return false; }
  pBLEScan->setAdvertisedDeviceCallbacks(&g_blecb);
  pBLEScan->setActiveScan(false);
  pBLEScan->setInterval(80);
  pBLEScan->setWindow(80);
  g_bleReady = true;
  Serial.println("[BLE] ready");
  return true;
}

// ============================================================================
// EVENT BUILDERS
// ============================================================================
static void fillEvent(FootfallEvent& ev, const char* type) {
  memset(&ev, 0, sizeof(ev));
  copyStr(ev.schema_version, 8, SCHEMA_VERSION);
  genEventId(ev.event_id);
  copyStr(ev.event_type, 24, type);
  copyStr(ev.device_id, 32, DEVICE_ID);
  copyStr(ev.site_id, 32, SITE_ID);
  copyStr(ev.asset_id, 32, ASSET_ID);
  copyStr(ev.asset_type, 24, ASSET_TYPE);
  copyStr(ev.creative_id, 32, CREATIVE_ID);
  ev.campaign_id = CAMPAIGN_ID;
  copyStr(ev.activation_name, 48, ACTIVATION);
  copyStr(ev.uplink_type, 20, "wifi_hotspot");
  copyStr(ev.fw_version, 24, FW_VERSION);

  uint64_t utc = nowUtc();
  ev.timestamp_epoch = utc;
  epochToIso(utc, ev.timestamp_utc);

  if (g_fix && g_lastUtc != 0 && utc != 0 && (utc - g_lastUtc) <= 30) {
    ev.gps_fix = true; ev.lat = g_lat; ev.lon = g_lon;
  }
}

// ============================================================================
// JSON
// ============================================================================
static String buildBatch(FootfallEvent* evs, uint8_t n) {
  String p; p.reserve(n * 700 + 32);
  p += "{\"events\":[";
  for (uint8_t i = 0; i < n; i++) {
    char hx[65]; hashHex(evs[i].mac_hash, hx);
    if (i) p += ",";
    p += "{";
    p += "\"schema_version\":\""; p += evs[i].schema_version; p += "\",";
    p += "\"event_id\":\"";       p += evs[i].event_id;        p += "\",";
    p += "\"event_type\":\"";     p += evs[i].event_type;      p += "\",";
    p += "\"device_id\":\"";      p += evs[i].device_id;       p += "\",";
    p += "\"site_id\":\"";        p += evs[i].site_id;         p += "\",";
    p += "\"asset_id\":\"";       p += evs[i].asset_id;        p += "\",";
    p += "\"asset_type\":\"";     p += evs[i].asset_type;      p += "\",";
    p += "\"creative_id\":\"";    p += evs[i].creative_id;     p += "\",";
    p += "\"campaign_id\":";      p += evs[i].campaign_id;     p += ",";
    p += "\"activation_name\":\"";p += evs[i].activation_name; p += "\",";
    p += "\"timestamp_epoch\":";  p += String((unsigned long long)evs[i].timestamp_epoch); p += ",";
    p += "\"timestamp_utc\":\"";  p += evs[i].timestamp_utc;   p += "\",";
    p += "\"session_start_epoch\":"; p += String((unsigned long long)evs[i].session_start_epoch); p += ",";
    p += "\"session_end_epoch\":";   p += String((unsigned long long)evs[i].session_end_epoch);   p += ",";
    p += "\"session_start_utc\":\""; p += evs[i].session_start_utc; p += "\",";
    p += "\"session_end_utc\":\"";   p += evs[i].session_end_utc;   p += "\",";
    p += "\"mac_hash\":\"";       p += hx;                     p += "\",";
    p += "\"signal_source\":\"";  p += evs[i].signal_source;   p += "\",";
    p += "\"rssi\":";             p += (int)evs[i].rssi;        p += ",";
    p += "\"dwell_time_sec\":";   p += evs[i].dwell_time_sec;   p += ",";
    p += "\"gps_fix\":";          p += evs[i].gps_fix ? "true":"false"; p += ",";
    if (evs[i].gps_fix) {
      p += "\"lat\":"; p += String(evs[i].lat,7); p += ",";
      p += "\"lon\":"; p += String(evs[i].lon,7); p += ",";
    } else { p += "\"lat\":null,\"lon\":null,"; }
    p += "\"uplink_type\":\""; p += evs[i].uplink_type; p += "\",";
    p += "\"fw_version\":\"";  p += evs[i].fw_version;  p += "\"";
    p += "}";
  }
  p += "]}"; return p;
}

static String buildHeartbeat() {
  uint64_t ts = nowUtc(); char tsUtc[21]; epochToIso(ts, tsUtc);
  String b; b.reserve(800);
  b += "{\"schema_version\":\"1.0\",";
  b += "\"device_id\":\"";    b += DEVICE_ID;   b += "\",";
  b += "\"site_id\":\"";      b += SITE_ID;     b += "\",";
  b += "\"asset_id\":\"";     b += ASSET_ID;    b += "\",";
  b += "\"asset_type\":\"";   b += ASSET_TYPE;  b += "\",";
  b += "\"fw_version\":\"";   b += FW_VERSION;  b += "\",";
  b += "\"uplink_type\":\"wifi_hotspot\",";
  b += "\"ota_channel\":\"stable\",";
  b += "\"timestamp_epoch\":"; b += String((unsigned long long)ts); b += ",";
  b += "\"timestamp_utc\":\""; b += tsUtc; b += "\",";
  b += "\"gps_fix\":";  b += g_fix?"true":"false"; b += ",";
  if (g_fix) {
    b += "\"lat\":"; b += String((double)g_lat,7); b += ",";
    b += "\"lon\":"; b += String((double)g_lon,7); b += ",";
  } else { b += "\"lat\":null,\"lon\":null,"; }
  b += "\"uptime_sec\":";       b += String((uint32_t)up_s());      b += ",";
  b += "\"queue_depth\":";      b += String(qDepth());              b += ",";
  b += "\"spool_bytes\":";      b += String((uint32_t)spoolSize()); b += ",";
  b += "\"dropped_presence\":"; b += String(g_droppedPresence);     b += ",";
  b += "\"dropped_exit\":";     b += String(g_droppedExit);         b += ",";
  b += "\"upload_failures\":";  b += String(g_uploadFailures);      b += ",";
  b += "\"total_presence\":";   b += String(g_totalPresence);       b += ",";
  b += "\"total_exit\":";       b += String(g_totalExit);           b += ",";
  b += "\"total_uploaded\":";   b += String(g_totalUploaded);       b += ",";
  b += "\"wifi_rssi\":";        b += String(WiFi.RSSI());           b += ",";
  b += "\"wifi_status\":";      b += String((int)WiFi.status());    b += ",";
  b += "\"modem_ready\":";      b += g_modemReady?"true":"false";   b += ",";
  b += "\"gnss_ready\":";       b += g_gnssReady?"true":"false";
  b += "}"; return b;
}

// ============================================================================
// PRESENCE + DWELL
// ============================================================================
static void presenceTick() {
  uint64_t t = up_s(); uint16_t pushed = 0;
  for (int i = 0; i < MAX_DEVICES; i++) {
    portENTER_CRITICAL(&g_tabMux);
    if (!g_tab[i].used) { portEXIT_CRITICAL(&g_tabMux); continue; }
    uint64_t age = t > g_tab[i].last_seen_up_s ? t-g_tab[i].last_seen_up_s : 0;
    if (age > PRESENCE_RECENT_SEC) { portEXIT_CRITICAL(&g_tabMux); continue; }
    DeviceEntry snap = g_tab[i];
    portEXIT_CRITICAL(&g_tabMux);

    FootfallEvent ev; fillEvent(ev, "presence");
    memcpy(ev.mac_hash, snap.mac_hash, 32);
    ev.rssi = snap.last_rssi; ev.dwell_time_sec = 0;
    copyStr(ev.signal_source, 16, snap.signal_source);
    if (!qPush(ev)) { g_droppedPresence++; continue; }
    g_totalPresence++;
    if (++pushed >= PRESENCE_MAX_PER_TICK) break;
  }
}

static void dwellSweep() {
  uint64_t t = up_s();
  for (int i = 0; i < MAX_DEVICES; i++) {
    portENTER_CRITICAL(&g_tabMux);
    if (!g_tab[i].used) { portEXIT_CRITICAL(&g_tabMux); continue; }
    uint64_t gap = t > g_tab[i].last_seen_up_s ? t-g_tab[i].last_seen_up_s : 0;
    if (gap < EXIT_THRESHOLD_SEC) { portEXIT_CRITICAL(&g_tabMux); continue; }
    DeviceEntry snap = g_tab[i]; g_tab[i].used = false;
    portEXIT_CRITICAL(&g_tabMux);

    uint32_t dwell = snap.last_seen_up_s > snap.first_seen_up_s
      ? (uint32_t)(snap.last_seen_up_s - snap.first_seen_up_s) : 0;
    if (dwell < MIN_DWELL_SEC) continue;

    FootfallEvent ev; fillEvent(ev, "exposure_exit");
    memcpy(ev.mac_hash, snap.mac_hash, 32);
    ev.rssi = snap.last_rssi; ev.dwell_time_sec = dwell;
    copyStr(ev.signal_source, 16, snap.signal_source);

    uint64_t utc = nowUtc();
    if (utc) {
      uint64_t age = t > snap.last_seen_up_s ? t-snap.last_seen_up_s : 0;
      uint64_t end = utc > age ? utc-age : 0;
      uint64_t sta = end > dwell ? end-dwell : 0;
      ev.session_start_epoch = sta; ev.session_end_epoch = end;
      epochToIso(sta, ev.session_start_utc);
      epochToIso(end, ev.session_end_utc);
    }
    if (!qPush(ev)) g_droppedExit++; else g_totalExit++;
  }
}

// ============================================================================
// TLS HTTP POST
// ============================================================================
static bool tlsPost(const char* path, const String& body) {
  if (!wifiConnect()) return false;
  IPAddress ip;
  if (!resolveBackend(ip)) return false;

  WiFiClientSecure cli; cli.setInsecure(); cli.setTimeout(TLS_TIMEOUT_MS);
  if (!cli.connect(ip, BACKEND_PORT)) {
    Serial.println("[TLS] connect failed"); return false;
  }

  String req; req.reserve(body.length()+300);
  req += "POST "; req += path; req += " HTTP/1.1\r\n";
  req += "Host: "; req += BACKEND_HOST; req += "\r\n";
  req += "Authorization: Bearer "; req += DEVICE_TOKEN; req += "\r\n";
  req += "Content-Type: application/json\r\n";
  req += "Content-Length: "; req += body.length(); req += "\r\n";
  req += "Connection: close\r\n\r\n";
  req += body;
  cli.print(req);

  uint32_t t0 = millis();
  while (cli.connected() && !cli.available() && millis()-t0 < TLS_TIMEOUT_MS) delay(20);
  if (!cli.available()) { cli.stop(); return false; }

  String status = cli.readStringUntil('\n'); status.trim();
  Serial.printf("[TLS] %s → %s\n", path, status.c_str());
  bool ok = status.indexOf("200")>=0 || status.indexOf("201")>=0 || status.indexOf("204")>=0;
  while (cli.available()) cli.readStringUntil('\n');
  cli.stop();
  if (ok) g_totalUploaded++;
  return ok;
}

// ============================================================================
// UPLOAD
// ============================================================================
static bool uploadBurst() {
  bool any = false;
  String line;
  if (spoolPop(line) && line.length()) {
    if (!tlsPost(PATH_INGEST, line)) {
      spoolAppend(line); g_uploadFailures++; return false;
    }
    any = true;
  }
  uint8_t n = 0; FootfallEvent ev;
  while (n < BATCH_MAX_EVENTS && qPop(ev)) g_uploadBatch[n++] = ev;
  if (n) {
    String b = buildBatch(g_uploadBatch, n);
    Serial.printf("[UPLOAD] batch n=%u  %u bytes\n", n, b.length());
    if (!tlsPost(PATH_INGEST, b)) {
      spoolAppend(b); g_uploadFailures++; return false;
    }
    any = true;
  }
  return any;
}

static void schedNext(bool ok) {
  if (ok) {
    g_backoffMs = BACKOFF_MIN_MS;
    g_nextUploadMs = millis() + UPLOAD_PERIOD_MS;
  } else {
    g_backoffMs = min(g_backoffMs*2u, BACKOFF_MAX_MS);
    g_nextUploadMs = millis() + g_backoffMs;
    Serial.printf("[BACKOFF] next in %ums\n", g_backoffMs);
  }
}

// ============================================================================
// STATUS PRINT (every 10s for field debug)
// ============================================================================
static void printStatus() {
  if (millis() - g_lastStatusMs < STATUS_PRINT_MS) return;
  g_lastStatusMs = millis();
  Serial.println("=========== STATUS ===========");
  Serial.printf("  Board:     7SEMI ESP32-S3 EC200U v2.2\n");
  Serial.printf("  FW:        %s\n", FW_VERSION);
  Serial.printf("  Uptime:    %llus\n", (unsigned long long)up_s());
  Serial.printf("  WiFi:      %s  RSSI=%ddBm\n",
                wifiName(WiFi.status()), WiFi.RSSI());
  Serial.printf("  Modem:     %s   GNSS: %s\n",
                g_modemReady?"OK":"FAIL", g_gnssReady?"OK":"FAIL");
  Serial.printf("  GPS fix:   %s  lat=%.6f lon=%.6f\n",
                g_fix?"YES":"NO", (double)g_lat, (double)g_lon);
  Serial.printf("  Q depth:   %u events\n", qDepth());
  Serial.printf("  Presence:  %u total  %u dropped\n", g_totalPresence,  g_droppedPresence);
  Serial.printf("  Exits:     %u total  %u dropped\n", g_totalExit,      g_droppedExit);
  Serial.printf("  Uploads:   %u OK  %u fail\n",       g_totalUploaded,  g_uploadFailures);
  Serial.printf("  Spool:     %uB\n", (unsigned)spoolSize());
  Serial.println("==============================");
}

// ============================================================================
// SETUP
// ============================================================================
void setup() {
  Serial.begin(115200);
  delay(1000);
  Serial.println("\n#############################################");
  Serial.println("  PILOT FIRMWARE v9");
  Serial.println("  7SEMI ESP32-S3 EC200U v2.2");
  Serial.println("  BLE + GNSS + WiFi → Railway Backend");
  Serial.println("#############################################\n");

  // NVS
  esp_err_t nvs = nvs_flash_init();
  if (nvs == ESP_ERR_NVS_NO_FREE_PAGES || nvs == ESP_ERR_NVS_NEW_VERSION_FOUND) {
    nvs_flash_erase(); nvs_flash_init();
  }

  // Salt for MAC hashing
  for (int i = 0; i < 16; i++) g_salt[i] = (uint8_t)esp_random();

  // SPIFFS
  g_spiffs_ok = SPIFFS.begin(true);
  Serial.printf("[SPIFFS] %s  spool=%uB\n", g_spiffs_ok?"OK":"FAIL",(unsigned)spoolSize());

  // WiFi + DNS
  wifiInit();
  if (wifiConnect()) {
    IPAddress ip; resolveBackend(ip);
  }

  // Modem + GNSS
  g_modemReady = modemInit();
  Serial.printf("[MODEM] %s\n", g_modemReady?"OK":"FAIL");
  if (g_modemReady) {
    g_gnssReady = gnssStart();
    Serial.printf("[GNSS]  %s\n", g_gnssReady?"OK":"FAIL");
  }

  // BLE (after modem settled)
  bleInit();

  uint32_t now = millis();
  g_lastGnssMs = g_lastGnssRetryMs = g_lastBleKickMs =
  g_lastSweepMs = g_lastPresenceMs = g_lastHeartbeatMs =
  g_lastStatusMs = now;
  g_nextUploadMs = now + 5000;

  Serial.printf("\n[READY] running  backend=%s  device=%s\n\n",
                BACKEND_HOST, DEVICE_ID);
}

// ============================================================================
// LOOP
// ============================================================================
void loop() {
  uint32_t ms = millis();

  // BLE scan kick (every 1.2s)
  if (g_bleReady && pBLEScan && !pBLEScan->isScanning() &&
      ms - g_lastBleKickMs >= 1200) {
    g_lastBleKickMs = ms;
    pBLEScan->clearResults();
    pBLEScan->start(1, false);
  }

  // Dwell sweep (1s)
  if (ms - g_lastSweepMs >= 1000) { g_lastSweepMs = ms; dwellSweep(); }

  // Presence tick (1s)
  if (ms - g_lastPresenceMs >= PRESENCE_TICK_MS) {
    g_lastPresenceMs = ms; presenceTick();
  }

  // GNSS poll (5s)
  if (g_modemReady && g_gnssReady && ms - g_lastGnssMs >= GNSS_POLL_MS) {
    g_lastGnssMs = ms;
    bool fix = false; double lat=0,lon=0; uint64_t epoch=0;
    if (gnssPoll(fix, lat, lon, epoch)) {
      g_fix = fix;
      if (fix) {
        g_lat = lat; g_lon = lon;
        if (epoch > 0) { g_lastUtc = epoch; g_lastUtcUpS = up_s(); }
        Serial.printf("[GNSS] FIX  lat=%.7f  lon=%.7f  utc=%llu\n",
                      g_lat, g_lon, (unsigned long long)g_lastUtc);
      } else { Serial.println("[GNSS] no fix yet"); }
    }
  }

  // Modem/GNSS retry (30s)
  if ((!g_modemReady || !g_gnssReady) && ms - g_lastGnssRetryMs >= GNSS_RETRY_MS) {
    g_lastGnssRetryMs = ms;
    Serial.println("[RETRY] modem/gnss...");
    g_modemReady = modemInit();
    if (g_modemReady) g_gnssReady = gnssStart();
  }

  // Upload burst
  if ((int32_t)(ms - g_nextUploadMs) >= 0) schedNext(uploadBurst());

  // Heartbeat (60s)
  if (ms - g_lastHeartbeatMs >= HEARTBEAT_PERIOD_MS) {
    g_lastHeartbeatMs = ms;
    Serial.println("[HB] sending...");
    tlsPost(PATH_HEARTBEAT, buildHeartbeat());
  }

  // Status print (10s)
  printStatus();

  delay(5);
}
