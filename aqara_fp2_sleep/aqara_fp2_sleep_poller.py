#!/usr/bin/env python3
"""Poll Aqara FP2 sleep telemetry and publish Home Assistant MQTT discovery."""

from base64 import b64encode
from datetime import datetime, timezone
import hashlib
import json
import os
import re
import signal
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import uuid

import paho.mqtt.client as mqtt
from Crypto.Cipher import PKCS1_v1_5
from Crypto.Hash import MD5 as CMD5
from Crypto.PublicKey import RSA

# Aqara Home app built-in constants. These are public app constants, not user
# credentials. The USA and CN pairs were checked against their live hosts on
# 2026-10-01. scripts/validate_repository.py pins a fingerprint of every row
# (server, app id and key) and records how it was checked, so any change here
# fails the build until its fingerprint is updated, the prompt to check the
# row against its live host again.
AREAS = {
    "CN": {
        "server": "https://aiot-rpc.aqara.cn",
        "appid": "94549908487478b220992a70",
        "appkey": "euGhPe2rcmxwculATNj45eEtnd50zp0I",
    },
    "EU": {
        "server": "https://rpc-ger.aqara.com",
        "appid": "7be1984f0556276133336839",
        "appkey": "Jddz01kIORDYrBzqGYgpUXKBnIHfW8E3",
    },
    "RU": {
        "server": "https://rpc-ru.aqara.com",
        "appid": "94549908487478b220992a70",
        "appkey": "euGhPe2rcmxwculATNj45eEtnd50zp0I",
    },
    "KR": {
        "server": "https://rpc-kr.aqara.com",
        "appid": "94549908487478b220992a70",
        "appkey": "euGhPe2rcmxwculATNj45eEtnd50zp0I",
    },
    "USA": {
        "server": "https://aiot-rpc-usa.aqara.com",
        "appid": "94549908487478b220992a70",
        "appkey": "euGhPe2rcmxwculATNj45eEtnd50zp0I",
    },
}

PUBKEY = (
    "-----BEGIN PUBLIC KEY-----\n"
    "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCG46slB57013JJs4Vvj5cVyMpR\n"
    "9b+B2F+YJU6qhBEYbiEmIdWpFPpOuBikDs2FcPS19MiWq1IrmxJtkICGurqImRUt\n"
    "4lP688IWlEmqHfSxSRf2+aH0cH8VWZ2OaZn5DWSIHIPBF2kxM71q8stmoYiV0oZs\n"
    "rZzBHsMuBwA4LQdxBwIDAQAB\n"
    "-----END PUBLIC KEY-----"
)

SLEEP_OPTIONS = [
    "heartrate_value",
    "respiration_rate_value",
    "sleep_state",
    "body_movement_value",
    "lux",
    "set_device_mode4",
    "device_offline_status",
]

# The FP2's operating mode and offline flag ride along in the same query. The
# vitals only mean something while the FP2 is online and in Sleep Monitoring
# mode, so both are published as diagnostic entities. Aqara does not document
# the codes. The mode mapping follows the community AqaraPOST Node-RED flow
# (github.com/sdavides/AqaraPOST-Homeassistant); unmapped modes are shown raw.
# Despite its name, `device_offline_status` reads 1 while the FP2 is online:
# a live US FP2 in Sleep monitoring mode reported 1 with heart rate seconds
# old, last changed when it reconnected after a network restart. AqaraPOST's
# mapping of that flag is the other way round.
MODE_ATTR = "set_device_mode4"
OFFLINE_ATTR = "device_offline_status"
DEVICE_MODES = {"3": "Zone detection", "5": "Fall detection", "9": "Sleep monitoring"}
SLEEP_MONITOR_MODE = "9"
ONLINE_CODE = "1"
OFFLINE_CODE = "0"
ONLINE_STATES = {ONLINE_CODE: "ON", OFFLINE_CODE: "OFF"}

# Aqara's history log returns every value a resource reported, with its time,
# but takes numeric resource ids rather than the names res/query uses. These
# ids come from Darkdragon14/ha-aqara-devices and match absent42/Aqara-LANLink's
# FP2 catalogue. Once per start the app asks the log for the last two hours of
# each reading and logs what it holds, to tell whether readings missed between
# polls could be backfilled. Nothing is published from it yet.
HISTORY_LOG_PATH = "/app/v1.0/lumi/res/history/log"
HISTORY_RESOURCE_IDS = {
    "0.8.85": "heartrate_value",
    "0.9.85": "respiration_rate_value",
    "13.106.85": "sleep_state",
    "13.11.85": "body_movement_value",
}
HISTORY_CHECK_WINDOW_S = 2 * 3600
HISTORY_CHECK_PAGE_SIZE = 300
# The check also samples one hour of each of the past nights, ending at this
# local hour, to tell a bad night from a sensor that never measures.
HISTORY_NIGHTS = 7
HISTORY_NIGHT_HOUR = 2
HISTORY_NIGHT_WINDOW_S = 3600
# Pause between the past-night requests so the check is not a burst.
HISTORY_REQUEST_GAP_S = 1.0
# The check then scans heart rate through one whole past night, one window
# per hour from 19:00 to 07:00, to see how often the FP2 updates it.
HEART_RATE_ID = "0.8.85"
BREATHING_ID = "0.9.85"
HISTORY_SCAN_FIRST_HOUR = 20
HISTORY_SCAN_HOURS = 12
# The check also lists every sleep-stage change of the last 14 hours, so a
# night the app missed (a late start, an outage) can be reconstructed.
SLEEP_STATE_ID = "13.106.85"
HISTORY_STAGE_WINDOW_S = 14 * 3600
STAGE_LABELS = {
    "0": "out of bed",
    "1": "in bed",
    "2": "awake",
    "3": "REM",
    "4": "light",
    "5": "deep",
}

# Night totals. Aqara's sleep stage reaches its cloud 7-19 minutes after it
# happened, so Home Assistant's own time-in-state counts start and end late by
# different amounts. These totals are computed from Aqara's timestamps instead:
# the night's stage changes come from the history log, and the time from one
# change to the next counts toward the earlier change's stage. The latest
# change's stage is not counted until the next change arrives, so a total
# never runs ahead of what Aqara has confirmed, and the morning value is
# exact. The night starts at NIGHT_START_HOUR local time and runs 24 hours.
NIGHT_START_HOUR = 18
NIGHT_REBUILD_CHUNK_S = 2 * 3600   # window per request when rebuilding a night
NIGHT_MIN_CHUNK_S = 10 * 60        # a full page is split down to this window
NIGHT_REFRESH_WINDOW_S = 3600      # re-read each poll; covers Aqara's upload delay
NIGHT_TOTALS = [
    # (state key, object id suffix, name, stage codes, icon)
    ("night_asleep_h", "night_asleep", "Night asleep", frozenset({"3", "4", "5"}), "mdi:sleep"),
    ("night_light_h", "night_light_sleep", "Night light sleep", frozenset({"4"}), "mdi:weather-night-partly-cloudy"),
    ("night_deep_h", "night_deep_sleep", "Night deep sleep", frozenset({"5"}), "mdi:power-sleep"),
    ("night_rem_h", "night_rem_sleep", "Night REM sleep", frozenset({"3"}), "mdi:eye-outline"),
]

AREA = os.environ.get("AQARA_AREA", "EU").upper()
USER = os.environ.get("AQARA_USER", "")
PASSWORD = os.environ.get("AQARA_PASS", "")
SUBJECT = os.environ.get("SUBJECT_ID", "").strip()
INTERVAL = max(15, int(os.environ.get("POLL_INTERVAL", "60") or "60"))
DEVICE_NAME = os.environ.get("DEVICE_NAME", "Aqara FP2 Sleep Monitor").strip()

LEVELS = {
    "trace": 0,
    "debug": 1,
    "info": 2,
    "notice": 2,
    "warning": 3,
    "error": 4,
    "fatal": 4,
}
LOG_LEVEL = LEVELS.get(os.environ.get("LOG_LEVEL", "info"), 2)

MQTT_HOST = os.environ.get("MQTT_HOST", "core-mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883") or "1883")
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
MQTT_SSL = str(os.environ.get("MQTT_SSL", "false")).lower() in ("true", "1", "yes")
MQTT_CONNECT_RETRIES = 6
MQTT_CONNECT_BACKOFF = (
    5  # seconds; doubles each attempt, capped at MQTT_CONNECT_BACKOFF_MAX
)
MQTT_CONNECT_BACKOFF_MAX = 60

# Home Assistant core API, reached through the Supervisor proxy. Used only to
# raise and clear a persistent notification, and only when the add-on was
# granted homeassistant_api. Every call fails soft.
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
CORE_API = os.environ.get("CORE_API", "http://supervisor/core/api")

# Consecutive transient failures tolerated before the diagnostic entity flips
# to a problem. DNS blips against the Aqara endpoints are common enough on this
# hardware (URLError / Errno -3) that flagging the first one would notify the
# user about something the next poll fixes. A real outage still surfaces within
# TRANSIENT_FAILURE_GRACE * POLL_INTERVAL seconds. A permanent rejection never
# waits for this grace.
TRANSIENT_FAILURE_GRACE = max(1, int(os.environ.get("TRANSIENT_FAILURE_GRACE", "3")))

# Backoff between login attempts after Aqara has *rejected* the credentials, as
# opposed to failing to answer. Without it a wrong password or region retries
# every poll interval forever: at the default 60s that is ~2,880 rejected auth
# requests a day, which is how accounts get rate-limited or locked.
AUTH_RETRY_BACKOFF = max(INTERVAL, 60)
AUTH_RETRY_BACKOFF_MAX = 1800

# Aqara result codes that get their own message. Meanings follow Aqara's public
# error table and the msgDetails field of the response; everything else is
# classified structurally by is_transient_code().
# 106 "Invalid sign": Aqara rejected the request signature, which comes from
#     SleepRadar's built-in app key for the region. It says nothing about the
#     user's password or region.
AQARA_CODE_INVALID_SIGN = 106
# 108 "Token has expired": right after a fresh sign-in, another sign-in on the
#     same Aqara account has ended SleepRadar's session.
AQARA_CODE_TOKEN_EXPIRED = 108
# 755 no permission for the device: after a good sign-in, aqara_area is not the
#     region the FP2 is registered in, or subject_id points at another device.
AQARA_CODE_SUBJECT_PERMISSION_DENIED = 755

ISSUES_URL = "https://github.com/florianhorner/ha-fp2-sleep/issues"

# Failures that mean "Aqara did not answer" rather than "Aqara said no".
# -1 is this client's own catch-all for socket/DNS/timeout exceptions, see
# Aqara._post(). The HTTP codes are server-side and heal without user action.
TRANSIENT_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def sanitize_node_id(value):
    node = re.sub(r"[^a-z0-9_]+", "_", value.strip().lower())
    node = re.sub(r"_+", "_", node).strip("_")
    return node or "aqara_fp2_sleep"


DISCOVERY_PREFIX = "homeassistant"
NODE = sanitize_node_id(os.environ.get("MQTT_NODE_ID", "aqara_fp2_sleep"))
STATE_TOPIC = f"aqara/{NODE}/state"
AVAIL_TOPIC = f"aqara/{NODE}/status"

# Diagnostic surface. The vitals sensors go `unavailable` when anything breaks,
# which tells a user that something is wrong but never what or how to fix it.
# These topics carry the reason, so they are not gated by AVAIL_TOPIC: gating
# them would blank the reason at the moment it is needed.
PROBLEM_STATE_TOPIC = f"aqara/{NODE}/problem"
PROBLEM_ATTR_TOPIC = f"aqara/{NODE}/problem/attributes"
PROBLEM_OBJECT_ID = f"{NODE}_connection_problem"
NOTIFICATION_ID = f"{NODE}_connection_problem"
MODE_OBJECT_ID = f"{NODE}_fp2_mode"
ONLINE_OBJECT_ID = f"{NODE}_fp2_online"
# SleepRadar's own version, baked into the image by the Dockerfile from the
# Supervisor's BUILD_VERSION build argument.
APP_VERSION = os.environ.get("SLEEPRADAR_VERSION", "").strip() or "unknown"
# The device's sw_version is the FP2's own firmware, filled in from Aqara
# after the first good poll (see FP2_FIRMWARE_FIELDS); Home Assistant shows it
# as the device's firmware. SleepRadar's version goes in the origin, which
# Home Assistant shows as "MQTT … via SleepRadar <version>".
DEVICE = {
    "identifiers": [f"aqara_fp2_sleep_{NODE}"],
    "name": DEVICE_NAME or "Aqara FP2 Sleep Monitor",
    "manufacturer": "Aqara",
    "model": "FP2 (Sleep Monitor)",
}
ORIGIN = {"name": "SleepRadar", "sw": APP_VERSION}
DEVICE_DETAIL_PATH = "/app/v1.0/lumi/dev/query/detail"
FP2_FIRMWARE_FIELDS = ("firmwareVersion", "firmware_version", "fwVersion", "firmware")
EXPIRE_AFTER = max(180, INTERVAL * 3)

SENSORS = [
    (
        "heartrate_value",
        f"{NODE}_heart_rate",
        "heart_rate",
        "Heart rate",
        "bpm",
        "measurement",
        None,
        "mdi:heart-pulse",
    ),
    (
        "respiration_rate_value",
        f"{NODE}_respiration_rate",
        "respiration_rate",
        "Respiration rate",
        "br/min",
        "measurement",
        None,
        "mdi:lungs",
    ),
    (
        "sleep_state",
        f"{NODE}_sleep_state",
        "sleep_state",
        "Sleep state",
        None,
        None,
        None,
        "mdi:sleep",
    ),
    (
        "body_movement_value",
        f"{NODE}_body_movement",
        "body_movement",
        "Body movement",
        None,
        "measurement",
        None,
        "mdi:run",
    ),
    (
        "lux",
        f"{NODE}_illuminance",
        "illuminance",
        "Illuminance",
        "lx",
        "measurement",
        "illuminance",
        "mdi:brightness-5",
    ),
]


def log(level, msg):
    if LEVELS.get(level, 2) >= LOG_LEVEL:
        print(f"[{level}] {msg}", flush=True)


def md5(s):
    return hashlib.md5(s.encode()).hexdigest()


class Aqara:
    def __init__(self, area):
        if area not in AREAS:
            fatal_startup(f"Unknown AQARA_AREA={area!r}; use one of {', '.join(AREAS)}")
        self.area = area
        self.cfg = AREAS[area]
        self.token = None
        self.userid = None
        # Last failing login response, kept so callers can tell "Aqara said no"
        # apart from "Aqara did not answer" without re-running the request.
        self.last_error = None

    def _headers(self, body):
        nonce = md5(str(uuid.uuid4()))
        now_ms = str(round(time.time() * 1000))
        appid = self.cfg["appid"]
        appkey = self.cfg["appkey"]
        parts = [f"Appid={appid}", f"Nonce={nonce}", f"Time={now_ms}"]
        if self.token:
            parts.append(f"Token={self.token}")
        if body:
            parts.append(body)
        parts.append(appkey)
        headers = {
            "Area": self.area,
            "Appid": appid,
            "Nonce": nonce,
            "Time": now_ms,
            "Sign": md5("&".join(parts)),
            "User-Agent": "pyAqara/1.0.0",
            "App-Version": "3.0.0",
            "Sys-Type": "1",
            "Lang": "en",
            "PhoneId": str(uuid.uuid4()).upper(),
        }
        if self.token:
            headers["Token"] = self.token
        if self.userid:
            headers["Userid"] = self.userid
        return headers

    def _post(self, path, body_obj):
        body = json.dumps(body_obj)
        headers = self._headers(body)
        headers["Content-Type"] = "application/json; charset=utf-8"
        req = urllib.request.Request(
            self.cfg["server"] + path,
            data=body.encode(),
            headers=headers,
            method="POST",
        )
        return self._send(req)

    def _get(self, path, query, sign_query):
        """GET `path?query`. The app signs some GETs without the query
        string and some with it, so `sign_query` picks which."""
        headers = self._headers(query if sign_query else None)
        req = urllib.request.Request(
            f"{self.cfg['server']}{path}?{query}", headers=headers, method="GET"
        )
        return self._send(req)

    def probe(self, method, path, query="", body_obj=None):
        """One request to `path` for the device-list probe: GET with an
        optional query (signed without it), or POST with a JSON body."""
        if method == "GET":
            return self._get(path, query, sign_query=False)
        return self._post(path, body_obj if body_obj is not None else {})

    def device_detail(self, did):
        """The device's details (model, firmware, ...) as the Aqara Home app
        reads them. Signed like the AqaraPOST flow (no query string in the
        signature); an "Invalid sign" reply is retried with the query signed."""
        query = urllib.parse.urlencode({"dids": did})
        res = self._get(DEVICE_DETAIL_PATH, query, sign_query=False)
        if res.get("code") == AQARA_CODE_INVALID_SIGN:
            res = self._get(DEVICE_DETAIL_PATH, query, sign_query=True)
        return res

    def _send(self, req):
        """Send a request; every failure comes back as a reply dict."""
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            # Every caller reads the reply with .get(). Valid JSON that is not
            # an object would end the poll loop, so it reads like any other
            # unreadable reply: Aqara did not answer.
            if not isinstance(body, dict):
                return {"code": -1, "message": "the reply was not a JSON object"}
            return body
        except urllib.error.HTTPError as err:
            try:
                body = json.loads(err.read().decode())
            except Exception:
                return {"code": err.code, "message": f"HTTP {err.code}"}
            # A gateway error page carries no usable Aqara code. Keep the HTTP
            # status, so a 429 or 5xx still reads as "no answer", not as a
            # rejection.
            if not isinstance(body, dict):
                return {"code": err.code, "message": f"HTTP {err.code}"}
            code = body.get("code")
            if not isinstance(code, int) or isinstance(code, bool):
                body["code"] = err.code
            return body
        except Exception as err:
            return {"code": -1, "message": f"{type(err).__name__}: {err}"}

    def login(self):
        # Every sign-in starts like the first one: an expired session's token
        # must not be signed into the request that replaces it.
        self.token = None
        self.userid = None
        rsa = PKCS1_v1_5.new(RSA.importKey(PUBKEY))
        encrypted_password = b64encode(
            rsa.encrypt(CMD5.new(PASSWORD.encode()).hexdigest().encode())
        ).decode()
        res = self._post(
            "/app/v1.0/lumi/user/login",
            {"account": USER, "encryptType": 2, "password": encrypted_password},
        )
        if res.get("code") == 0:
            result = res.get("result")
            if isinstance(result, dict) and result.get("token") and result.get("userId"):
                self.token = result["token"]
                self.userid = result["userId"]
                self.last_error = None
                log("info", "Aqara login OK")
                return True
            # Code 0 without a session is not a sign-in. Described, not kept:
            # the reply can hold a partial session token.
            res = {"code": -1, "message": "the sign-in reply had no session token"}
        self.last_error = res
        log("error", f"Aqara login failed: code={code_text(res.get('code'))} {aqara_error_text(res)}")
        return False

    def res_query(self, did, options):
        return self._post(
            "/app/v1.0/lumi/res/query",
            {"data": [{"options": options, "subjectId": did}]},
        )

    def history_log(
        self, did, resource_ids, start_ms, size=HISTORY_CHECK_PAGE_SIZE, scan_id="", end_ms=None
    ):
        """One page of the resources' reported values since `start_ms`.

        Same body shape as Aqara's documented fetch.resource.history and the
        AqaraPOST flow: `scanId` pages onward, `size` caps the page, and the
        optional `endTime` (documented, not yet seen honoured by the app API)
        ends the window.
        """
        body = {
            "resourceIds": list(resource_ids),
            "scanId": scan_id,
            "size": str(size),
            "startTime": int(start_ms),
            "subjectId": did,
        }
        if end_ms is not None:
            body["endTime"] = int(end_ms)
        return self._post(HISTORY_LOG_PATH, body)


def discovery_payload(
    attr, object_id, uid_suffix, name, unit, state_class, device_class, icon
):
    payload = {
        "name": name,
        "unique_id": f"{NODE}_{uid_suffix}",
        "object_id": object_id,
        "default_entity_id": f"sensor.{object_id}",
        "state_topic": STATE_TOPIC,
        "value_template": "{{ value_json.%s | default('unknown') }}" % attr,
        "icon": icon,
        "availability_topic": AVAIL_TOPIC,
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": EXPIRE_AFTER,
        # Without this, HA skips the state write (and last_updated stays
        # frozen) whenever a poll republishes an unchanged value — e.g. a
        # long stable deep-sleep reading — which would make the SleepRadar
        # Card's last_updated-based "stale" badge false-positive during
        # entirely normal operation.
        "force_update": True,
        # force_update also means last_updated only proves the poller is
        # alive. Aqara stamps each reading with the time the FP2 last
        # reported it, so that time rides along as the `measured_at`
        # attribute. Heart rate and breathing refresh every few seconds;
        # sleep_state only when the stage changes.
        "json_attributes_topic": STATE_TOPIC,
        "json_attributes_template": (
            "{{ {'measured_at': (value_json.measured_at | default({})).get('%s')}"
            " | tojson }}" % attr
        ),
        "origin": ORIGIN,
        "device": DEVICE,
    }
    if unit:
        payload["unit_of_measurement"] = unit
    if state_class:
        payload["state_class"] = state_class
    if device_class:
        payload["device_class"] = device_class
    return payload


def problem_discovery_payload():
    """Discovery for the diagnostic entity.

    Three differences from the vitals sensors, all on purpose:

    - no `availability_topic`. The vitals hang off AVAIL_TOPIC and go
      unavailable on any failure. This entity exists to explain that failure,
      so gating it on the same topic would blank it when it is needed.
    - no `expire_after`. The vitals expire so a dead poller stops showing a
      stale heart rate. This one must stay readable, and process death is
      covered by the MQTT will instead (see make_mqtt).
    - no `force_update`. That flag exists so the card's last_updated stale
      badge survives unchanged readings; a problem flag has no such consumer.
    """
    return {
        "name": "Connection problem",
        "unique_id": f"{NODE}_connection_problem",
        "object_id": PROBLEM_OBJECT_ID,
        "default_entity_id": f"binary_sensor.{PROBLEM_OBJECT_ID}",
        "state_topic": PROBLEM_STATE_TOPIC,
        "json_attributes_topic": PROBLEM_ATTR_TOPIC,
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "problem",
        "entity_category": "diagnostic",
        "icon": "mdi:cloud-alert",
        "origin": ORIGIN,
        "device": DEVICE,
    }


def device_status_discovery_payloads():
    """Discovery for the FP2's mode and online state, keyed by component.

    Both read the vitals' state topic and share their availability and
    expiry: they describe the same reply, so they go stale with it.
    """
    common = {
        "state_topic": STATE_TOPIC,
        "availability_topic": AVAIL_TOPIC,
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": EXPIRE_AFTER,
        "entity_category": "diagnostic",
        "origin": ORIGIN,
        "device": DEVICE,
    }
    mode = {
        "name": "FP2 mode",
        "unique_id": f"{NODE}_fp2_mode",
        "object_id": MODE_OBJECT_ID,
        "default_entity_id": f"sensor.{MODE_OBJECT_ID}",
        "value_template": "{{ value_json.device_mode | default('unknown') }}",
        "icon": "mdi:radar",
        **common,
    }
    online = {
        "name": "FP2 online",
        "unique_id": f"{NODE}_fp2_online",
        "object_id": ONLINE_OBJECT_ID,
        "default_entity_id": f"binary_sensor.{ONLINE_OBJECT_ID}",
        # An unreported flag renders "None", which Home Assistant reads as
        # unknown rather than offline.
        "value_template": "{{ value_json.fp2_online | default('None') }}",
        "payload_on": "ON",
        "payload_off": "OFF",
        "device_class": "connectivity",
        **common,
    }
    return {("sensor", "fp2_mode"): mode, ("binary_sensor", "fp2_online"): online}


def publish_discovery(client):
    for attr, object_id, uid_suffix, name, unit, sc, dc, icon in SENSORS:
        topic = f"{DISCOVERY_PREFIX}/sensor/{NODE}/{uid_suffix}/config"
        payload = discovery_payload(
            attr, object_id, uid_suffix, name, unit, sc, dc, icon
        )
        client.publish(topic, json.dumps(payload), qos=1, retain=True)
    client.publish(
        f"{DISCOVERY_PREFIX}/binary_sensor/{NODE}/connection_problem/config",
        json.dumps(problem_discovery_payload()),
        qos=1,
        retain=True,
    )
    device_status = device_status_discovery_payloads()
    for (component, suffix), payload in device_status.items():
        client.publish(
            f"{DISCOVERY_PREFIX}/{component}/{NODE}/{suffix}/config",
            json.dumps(payload),
            qos=1,
            retain=True,
        )
    for key, suffix, name, _codes, icon in NIGHT_TOTALS:
        client.publish(
            f"{DISCOVERY_PREFIX}/sensor/{NODE}/{suffix}/config",
            json.dumps(night_total_discovery_payload(key, suffix, name, icon)),
            qos=1,
            retain=True,
        )
    log(
        "info",
        f"Published discovery for {len(SENSORS)} sensors, {len(NIGHT_TOTALS)} "
        f"night totals and {1 + len(device_status)} diagnostic entities",
    )


def night_total_discovery_payload(key, suffix, name, icon):
    """Discovery for one night total: hours, from Aqara's own timestamps.

    No force_update: the value only changes when a stage change arrives, so
    unchanged polls need no Recorder row. It shares the vitals' availability
    and expiry, since it comes from the same poll.
    """
    object_id = f"{NODE}_{suffix}"
    return {
        "name": name,
        "unique_id": object_id,
        "object_id": object_id,
        "default_entity_id": f"sensor.{object_id}",
        "state_topic": STATE_TOPIC,
        "value_template": "{{ value_json.%s | default('None') }}" % key,
        "json_attributes_topic": STATE_TOPIC,
        "json_attributes_template": "{{ {'night_start': value_json.night_start | default(none)} | tojson }}",
        "unit_of_measurement": "h",
        "device_class": "duration",
        "state_class": "measurement",
        "icon": icon,
        "availability_topic": AVAIL_TOPIC,
        "payload_available": "online",
        "payload_not_available": "offline",
        "expire_after": EXPIRE_AFTER,
        "origin": ORIGIN,
        "device": DEVICE,
    }


def aqara_time_seconds(raw):
    """Aqara's `timeStamp` (epoch milliseconds) as epoch seconds, or None.

    Seconds are accepted as well. A value that is not a plain number, or that
    lands outside 2001-2096, reads as no timestamp rather than a wrong one.
    """
    if isinstance(raw, bool):
        return None
    if isinstance(raw, str):
        raw = raw.strip()
        if not (raw.isascii() and raw.isdigit()):
            return None
        raw = int(raw)
    if not isinstance(raw, (int, float)):
        return None
    seconds = raw / 1000 if raw >= 1e11 else raw
    if not 1e9 <= seconds <= 4e9:
        return None
    return float(seconds)


def aqara_time_iso(raw):
    """Aqara's `timeStamp` as ISO 8601 UTC, or None."""
    seconds = aqara_time_seconds(raw)
    if seconds is None:
        return None
    return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="seconds")


def code_string(raw):
    """A small integer code, sent by Aqara as a number or a string, as its
    decimal string, or None."""
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.isascii() and raw.isdigit():
            return str(int(raw))
    return None


def describe_mode(mode):
    """The FP2 mode's label, the raw code when unmapped, or None."""
    if mode is None:
        return None
    return DEVICE_MODES.get(mode, f"Unknown ({mode})")


def build_state(items):
    """(state payload, mode code, offline code) from a res/query result list.

    Entries that are not objects with a string `attr` are skipped, so an
    off-contract entry cannot end the poll loop.
    """
    readings = {
        item["attr"]: item
        for item in items
        if isinstance(item, dict) and isinstance(item.get("attr"), str)
    }
    state = {attr: readings.get(attr, {}).get("value") for attr, *_ in SENSORS}
    state["measured_at"] = {
        attr: aqara_time_iso(readings.get(attr, {}).get("timeStamp"))
        for attr, *_ in SENSORS
    }
    mode = code_string(readings.get(MODE_ATTR, {}).get("value"))
    offline = code_string(readings.get(OFFLINE_ATTR, {}).get("value"))
    state["device_mode"] = describe_mode(mode)
    state["fp2_online"] = ONLINE_STATES.get(offline)
    return state, mode, offline


def describe_reply_fields(items, now=None):
    """One log line naming the fields Aqara's readings carry and how old each
    measurement is. Field names and ages only: the reply also carries the
    sensor's id, which stays out of non-debug logs."""
    now = time.time() if now is None else now
    entries = [item for item in items if isinstance(item, dict)]
    fields = ", ".join(sorted({plain_text(key)[:40] for e in entries for key in e}))
    ages = {}
    for entry in entries:
        seconds = aqara_time_seconds(entry.get("timeStamp"))
        if seconds is not None:
            ages[plain_text(entry.get("attr"))[:40]] = round(now - seconds)
    if not ages:
        return (
            f"Aqara readings carry the fields: {fields or 'none'}. None has a "
            "usable timeStamp, so measured_at stays empty."
        )
    age_text = ", ".join(f"{attr} {age}s" for attr, age in sorted(ages.items()))
    return (
        f"Aqara readings carry the fields: {fields}. Measurement age per "
        f"reading: {age_text}."
    )


def device_status_message(mode, offline):
    """(level, text) describing the FP2's mode and online flag."""
    mode_text = describe_mode(mode) or "not reported"
    if offline == OFFLINE_CODE:
        return (
            "warning",
            f"Aqara reports the FP2 offline (mode: {mode_text}). Readings stop "
            "changing until it reconnects.",
        )
    if mode is not None and mode != SLEEP_MONITOR_MODE:
        return (
            "warning",
            f"The FP2 is in {mode_text} mode, not Sleep monitoring. Heart rate, "
            "breathing and sleep state stay empty or stale until it is switched "
            "back in the Aqara Home app.",
        )
    online_text = {ONLINE_CODE: "online"}.get(offline, "online state not reported")
    return "info", f"FP2 mode: {mode_text}; {online_text}."


def has_session(aqara):
    """Whether the client holds a session token. A test double without a
    `token` attribute counts as signed in."""
    return bool(getattr(aqara, "token", True))


def firmware_from_detail(res, did):
    """(firmware, field names) from a device-detail reply. firmware is plain
    text or None; the field names (never values) are for the log."""
    if res.get("code") != 0:
        return None, f"Aqara code {code_text(res.get('code'))}: {aqara_error_text(res)}"
    result = res.get("result")
    if isinstance(result, dict):
        result = result.get("data", [result])
    if not isinstance(result, list):
        return None, f"unreadable reply (result is a {type(result).__name__})"
    devices = [item for item in result if isinstance(item, dict)]
    mine = [d for d in devices if d.get("did", d.get("subjectId", did)) == did] or devices[:1]
    if not mine:
        return None, "no device in the reply"
    fields = ", ".join(sorted(plain_text(key)[:30] for key in mine[0]))
    for name in FP2_FIRMWARE_FIELDS:
        value = mine[0].get(name)
        if isinstance(value, (str, int, float)) and not isinstance(value, bool) and str(value).strip():
            return plain_text(value)[:40], fields
    return None, fields


# Candidate endpoints for listing the devices on an account, found in the
# Aqara Home iOS app. Probed once per start; only the reply's shape is logged.
DEVICE_LIST_PROBES = (
    ("GET", "/app/v1.0/lumi/app/dev/query/detail", ""),
    ("GET", "/app/v1.0/lumi/dev/query/detail", ""),
    ("GET", "/app/v1.0/lumi/app/position/device/query", ""),
    ("POST", "/app/v1.0/lumi/app/position/device/query", None),
    ("GET", "/app/v1.0/lumi/app/position/query", ""),
)


def describe_device_list_reply(res, did):
    """The shape of a device-list reply: code, where the list is, how many
    entries, their field names and models, and whether `did` is among them.
    No identifiers or other values."""
    code = res.get("code")
    if code != 0:
        return f"code {code_text(code)}: {aqara_error_text(res)}"
    result = res.get("result")
    found = []

    def walk(node, where, depth):
        if depth > 4:
            return
        if isinstance(node, list):
            dicts = [item for item in node if isinstance(item, dict)]
            if dicts:
                found.append((where or "result", dicts))
            for item in dicts[:3]:
                for key, value in item.items():
                    if isinstance(value, (list, dict)):
                        walk(value, f"{where} each {key}", depth + 1)
        elif isinstance(node, dict):
            for key, value in node.items():
                if isinstance(value, (list, dict)):
                    walk(value, f"{where}.{key}" if where else key, depth + 1)

    walk(result, "", 0)
    if not found:
        keys = ", ".join(sorted(plain_text(k)[:30] for k in result)) if isinstance(result, dict) else ""
        return f"no list (result is a {type(result).__name__}{'; fields: ' + keys if keys else ''})"
    parts = []
    for where, items in found[:4]:
        fields = sorted({plain_text(k)[:30] for item in items for k in item})
        models = sorted({plain_text(item.get("model"))[:40] for item in items if item.get("model")})
        mine = any(item.get("did") == did or item.get("subjectId") == did for item in items)
        parts.append(
            f"{plain_text(where)[:60]}: {len(items)} entries, fields {', '.join(fields)[:400]}; "
            f"models {', '.join(models)[:200] or 'none'}; this FP2 listed: {'yes' if mine else 'no'}"
        )
    return " | ".join(parts)


def probe_device_lists(aqara):
    """Try each candidate device-list endpoint once and log its shape."""
    probe = getattr(aqara, "probe", None)
    if probe is None:
        return
    for method, path, query in DEVICE_LIST_PROBES:
        if not _running:
            return
        time.sleep(HISTORY_REQUEST_GAP_S)
        try:
            res = probe(method, path, query)
            if not isinstance(res, dict):
                res = {"code": -1, "message": "the reply was not a JSON object"}
        except Exception as err:  # noqa: BLE001 - a probe must not stop polling
            res = {"code": -1, "message": f"{type(err).__name__}: {err}"}
        log("info", f"Device list probe {method} {path}: {describe_device_list_reply(res, SUBJECT)}")


def read_fp2_firmware(aqara, client):
    """Ask Aqara for the FP2's firmware once, show it as the device's
    firmware in Home Assistant, and log it. Never raises."""
    device_detail = getattr(aqara, "device_detail", None)
    if device_detail is None:
        return
    try:
        firmware, fields = firmware_from_detail(device_detail(SUBJECT), SUBJECT)
    except Exception as err:  # noqa: BLE001 - firmware is cosmetic
        log("info", f"FP2 firmware not read: {type(err).__name__}: {err}")
        return
    if firmware is None:
        log("info", f"FP2 firmware not found in Aqara's device details ({fields})")
        return
    log("info", f"FP2 firmware: {firmware}")
    if DEVICE.get("sw_version") != firmware:
        DEVICE["sw_version"] = firmware
        publish_discovery(client)


def night_start(now):
    """Epoch seconds of the night `now` belongs to: the latest local
    NIGHT_START_HOUR:00 at or before `now`."""
    lt = time.localtime(now)
    start = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, NIGHT_START_HOUR, 0, 0, 0, 0, -1))
    if start > now:
        start = time.mktime(
            (lt.tm_year, lt.tm_mon, lt.tm_mday - 1, NIGHT_START_HOUR, 0, 0, 0, 0, -1)
        )
    return start


class NightStages:
    """The current night's sleep-stage changes from Aqara's history log, and
    the hours spent in each stage by Aqara's own timestamps."""

    def __init__(self):
        self.start = None
        self.events = {}      # epoch seconds -> stage code
        self.complete = False

    def update(self, history_log, now):
        """Read new stage changes. A new night (or one not yet read in full)
        is rebuilt from its start; otherwise the last hour is re-read, which
        catches changes Aqara uploads late."""
        start = night_start(now)
        if start != self.start:
            self.start, self.events, self.complete = start, {}, False
        if not self.complete:
            self.complete = self._read(history_log, start, now, NIGHT_REBUILD_CHUNK_S)
        else:
            self._read(history_log, max(start, now - NIGHT_REFRESH_WINDOW_S), now, NIGHT_REFRESH_WINDOW_S)

    def _read(self, history_log, begin, end, chunk):
        """Merge the changes between `begin` and `end` in `chunk`-long
        requests. A full page is split in half until it fits. Returns False
        when any part could not be read in full."""
        complete = True
        t = begin
        while t < end:
            stop = min(end, t + chunk)
            res = _history_request(history_log, SLEEP_STATE_ID, int(t * 1000), end_ms=int(stop * 1000))
            readings, page, problem = history_readings(res, SLEEP_STATE_ID)
            if problem:
                log("debug", f"night totals: stage history unavailable ({problem})")
                return False
            if page >= HISTORY_CHECK_PAGE_SIZE and stop - t > NIGHT_MIN_CHUNK_S:
                complete = self._read(history_log, t, stop, (stop - t) / 2) and complete
            else:
                if page >= HISTORY_CHECK_PAGE_SIZE:
                    complete = False
                for seconds, value in readings:
                    code = code_string(value)
                    if code is not None and t - 1 <= seconds <= stop + 1:
                        self.events[seconds] = code
            t = stop
        return complete

    def totals(self):
        """{state key: hours} for NIGHT_TOTALS, or None until the night has
        been read in full. Each change's stage counts until the next change;
        the latest change is open and not counted yet."""
        if not self.complete:
            return None
        ordered = sorted(self.events.items())
        hours = {key: 0.0 for key, *_ in NIGHT_TOTALS}
        for (begin, code), (end, _next) in zip(ordered, ordered[1:]):
            for key, _suffix, _name, codes, _icon in NIGHT_TOTALS:
                if code in codes:
                    hours[key] += (end - begin) / 3600
        return {key: round(value, 3) for key, value in hours.items()}


def apply_night_totals(state, night, aqara, now=None):
    """Update `night` from the history log and add its totals to `state`.
    Never raises: a failure leaves the totals unknown for this poll."""
    totals = None
    history_log = getattr(aqara, "history_log", None)
    if history_log is not None:
        try:
            night.update(history_log, time.time() if now is None else now)
            totals = night.totals()
        except Exception as err:  # noqa: BLE001 - totals must not stop polling
            log("debug", f"night totals failed: {type(err).__name__}: {err}")
    for key, *_ in NIGHT_TOTALS:
        state[key] = totals.get(key) if totals else None
    state["night_start"] = (
        datetime.fromtimestamp(night.start, timezone.utc).isoformat(timespec="seconds")
        if night.start is not None
        else None
    )


def history_readings(res, resource_id):
    """(readings, page size, problem) from a history-log reply.

    readings are (epoch seconds, value) for this resource, oldest first; only
    entries with a usable time count. problem is a short description when the
    reply is a rejection or cannot be read, else None.
    """
    code = res.get("code")
    if code != 0:
        return [], 0, f"Aqara code {code_text(code)}: {aqara_error_text(res)}"
    result = res.get("result")
    data = result.get("data") if isinstance(result, dict) else None
    if not isinstance(data, list):
        fields = ""
        if isinstance(result, dict):
            fields = "; fields: " + (
                ", ".join(sorted(plain_text(key)[:30] for key in result)) or "none"
            )
        return [], 0, f"unreadable reply (result is a {type(result).__name__}{fields})"
    readings = sorted(
        (seconds, entry.get("value"))
        for entry in data
        if isinstance(entry, dict) and entry.get("resourceId", resource_id) == resource_id
        for seconds in (aqara_time_seconds(entry.get("timeStamp")),)
        if seconds is not None
    )
    return readings, len(data), None


def describe_values(readings):
    """The distinct integer values seen, as a/b/c or as a min–max range."""
    numbers = sorted({int(v) for v in (code_string(value) for _, value in readings) if v})
    if not numbers:
        return "no integer values"
    if len(numbers) > 3:
        return f"{numbers[0]}–{numbers[-1]}"
    return "/".join(str(number) for number in numbers)


def summarize_history_reply(res, resource_id, now):
    """One line describing a history-log page for one resource: how many
    entries, how far back, how densely and over which values. The reply also
    carries the sensor's id, which stays out of it."""
    label = f"{HISTORY_RESOURCE_IDS.get(resource_id, resource_id)} ({resource_id})"
    readings, page, problem = history_readings(res, resource_id)
    if problem:
        return f"{label}: {problem}"
    stamps = [seconds for seconds, _ in readings]
    if not stamps:
        return f"{label}: no entries in the last {HISTORY_CHECK_WINDOW_S // 3600} h"
    text = (
        f"{label}: {len(stamps)} entries, from {round((now - stamps[0]) / 60)} min "
        f"ago to {round(now - stamps[-1])} s ago"
    )
    gaps = sorted(later - earlier for earlier, later in zip(stamps, stamps[1:]))
    if gaps:
        text += f", median gap {round(gaps[len(gaps) // 2])} s"
    text += f", values {describe_values(readings)}"
    if page >= HISTORY_CHECK_PAGE_SIZE:
        text += "; page full, so older entries exist"
    return text


def night_sample_ends(now, nights=HISTORY_NIGHTS, hour=HISTORY_NIGHT_HOUR):
    """Epoch seconds of the last `nights` local `hour`:00 moments before
    `now`, newest first. mktime normalises the day, so month starts and DST
    changes need no special case."""
    lt = time.localtime(now)
    ends = []
    for back in range(nights + 1):
        end = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday - back, hour, 0, 0, 0, 0, -1))
        if end <= now:
            ends.append(end)
    return ends[:nights]


def describe_night_window(res, resource_id, start_s, end_s):
    """A short fragment for one resource in one past-night window."""
    name = HISTORY_RESOURCE_IDS.get(resource_id, resource_id)
    readings, _page, problem = history_readings(res, resource_id)
    if problem:
        return f"{name} {problem}"
    inside = [r for r in readings if start_s - 1 <= r[0] <= end_s + 1]
    if readings and not inside:
        return f"{name} {len(readings)} entries, all outside the window (endTime not honoured)"
    if not inside:
        return f"{name} none"
    return f"{name} {len(inside)} entries, values {describe_values(inside)}"


def readings_spread(readings):
    """Max minus min of the integer values, or -1 when there are none."""
    numbers = [int(v) for v in (code_string(value) for _, value in readings) if v]
    return max(numbers) - min(numbers) if numbers else -1


def night_scan_ends(night_end_s, first_hour=HISTORY_SCAN_FIRST_HOUR, hours=HISTORY_SCAN_HOURS):
    """Ends of the hour-long windows covering the night that `night_end_s`
    (that night's HISTORY_NIGHT_HOUR) falls in: from `first_hour`:00 the
    evening before, one per hour. mktime normalises hours past 23."""
    lt = time.localtime(night_end_s)
    return [
        time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday - 1, first_hour + i, 0, 0, 0, 0, -1))
        for i in range(hours)
    ]


def describe_scan_window(res, resource_id, start_s, end_s):
    """`HH:00 values (n changes)` for one hour of a full-night scan, labelled
    by the hour it ends."""
    label = f"{time.localtime(end_s).tm_hour:02d}:00"
    readings, _page, problem = history_readings(res, resource_id)
    if problem:
        return f"{label} {problem}"
    inside = [r for r in readings if start_s - 1 <= r[0] <= end_s + 1]
    if not inside:
        return f"{label} none"
    values = [code_string(value) for _, value in inside]
    changes = sum(1 for before, after in zip(values, values[1:]) if before != after)
    text = f"{label} {describe_values(inside)}"
    if changes:
        text += f" ({changes} change{'' if changes == 1 else 's'})"
    return text


def describe_stage_changes(res):
    """`HH:MM code label` for each sleep-stage entry, oldest first, in local
    time. Labels follow the community mapping SleepRadar documents."""
    readings, page, problem = history_readings(res, SLEEP_STATE_ID)
    if problem:
        return problem
    if not readings:
        return "none"
    parts = []
    for seconds, value in readings:
        code = code_string(value)
        label = STAGE_LABELS.get(code, "unmapped")
        parts.append(f"{time.strftime('%H:%M', time.localtime(seconds))} {code} {label}")
    text = ", ".join(parts)
    if page >= HISTORY_CHECK_PAGE_SIZE:
        text += "; page full, so older changes exist"
    return text


def _history_request(history_log, resource_id, start_ms, end_ms=None):
    """One history-log request that never raises."""
    try:
        if end_ms is None:
            res = history_log(SUBJECT, [resource_id], start_ms)
        else:
            res = history_log(SUBJECT, [resource_id], start_ms, end_ms=end_ms)
        if not isinstance(res, dict):
            res = {"code": -1, "message": "the reply was not a JSON object"}
    except Exception as err:  # noqa: BLE001 - a diagnostic must not stop polling
        res = {"code": -1, "message": f"{type(err).__name__}: {err}"}
    return res


def run_history_check(aqara, now=None):
    """Ask the history log once for the last two hours of each reading, and
    for an hour of each of the past nights, and log what it holds. Never
    raises, never touches Health or MQTT."""
    history_log = getattr(aqara, "history_log", None)
    if history_log is None:
        return
    now = time.time() if now is None else now
    start_ms = int((now - HISTORY_CHECK_WINDOW_S) * 1000)
    for resource_id in HISTORY_RESOURCE_IDS:
        res = _history_request(history_log, resource_id, start_ms)
        log("info", f"History check: {summarize_history_reply(res, resource_id, now)}")

    # Every sleep-stage change of the last HISTORY_STAGE_WINDOW_S, oldest
    # first. Stage changes are sparse, so one page holds a whole night, and
    # the list lets a night the app missed be reconstructed exactly.
    stage_start_ms = int((now - HISTORY_STAGE_WINDOW_S) * 1000)
    res = _history_request(history_log, SLEEP_STATE_ID, stage_start_ms, end_ms=int(now * 1000))
    log(
        "info",
        f"History check, sleep stage changes in the last "
        f"{HISTORY_STAGE_WINDOW_S // 3600} h: {describe_stage_changes(res)}",
    )

    # Past nights: one hour ending at HISTORY_NIGHT_HOUR local time, so a
    # flat or missing reading can be told apart from a bad night.
    night_ends = night_sample_ends(now)
    best_end, best_spread = (night_ends[0] if night_ends else None), -1
    for end_s in night_ends:
        start_s = end_s - HISTORY_NIGHT_WINDOW_S
        parts = []
        for resource_id in HISTORY_RESOURCE_IDS:
            if not _running:
                return
            time.sleep(HISTORY_REQUEST_GAP_S)
            res = _history_request(
                history_log, resource_id, int(start_s * 1000), end_ms=int(end_s * 1000)
            )
            parts.append(describe_night_window(res, resource_id, start_s, end_s))
            if resource_id == BREATHING_ID:
                readings, _page, _problem = history_readings(res, resource_id)
                spread = readings_spread(
                    [r for r in readings if start_s - 1 <= r[0] <= end_s + 1]
                )
                # Newest first, so a tie keeps the more recent night.
                if spread > best_spread:
                    best_end, best_spread = end_s, spread
        stamp = time.strftime("%a %d %b %H:%M %Z", time.localtime(end_s))
        log("info", f"History check, hour ending {stamp}: " + "; ".join(parts))

    # Heart rate through one whole night: the past night whose breathing
    # varied most, since breathing shows the FP2 had a vitals lock then. One
    # hour-long window per hour of the night shows how often heart rate moves.
    if best_end is None:
        return
    parts = []
    for end_s in night_scan_ends(best_end):
        if not _running:
            return
        time.sleep(HISTORY_REQUEST_GAP_S)
        start_s = end_s - HISTORY_NIGHT_WINDOW_S
        res = _history_request(
            history_log, HEART_RATE_ID, int(start_s * 1000), end_ms=int(end_s * 1000)
        )
        parts.append(describe_scan_window(res, HEART_RATE_ID, start_s, end_s))
    evening = time.strftime("%a %d %b", time.localtime(best_end - 12 * 3600))
    reason = (
        f"breathing varied most, by {best_spread}"
        if best_spread > 0
        else "no night's breathing varied, so the most recent"
    )
    log(
        "info",
        f"History check, heart rate through the night of {evening} ({reason}; "
        "each hour shows its last ~30 min): " + "; ".join(parts),
    )


def publish_problem(client, problem, cause=None, code=None, last_success=None):
    """Publish the diagnostic state and its attributes, both retained.

    Retained so the reason survives a Home Assistant restart: an outage that
    started before the restart still explains itself afterwards.
    """
    client.publish(
        PROBLEM_STATE_TOPIC, "ON" if problem else "OFF", qos=1, retain=True
    )
    client.publish(
        PROBLEM_ATTR_TOPIC,
        json.dumps(
            {
                "cause": cause,
                "error_code": code,
                "configured_area": AREA,
                "last_successful_poll": last_success,
            }
        ),
        qos=1,
        retain=True,
    )


def make_mqtt():
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=NODE)
    except (AttributeError, TypeError):
        client = mqtt.Client(client_id=NODE)
    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)
    if MQTT_SSL:
        client.tls_set()
    # paho allows a single will, and the diagnostic entity is the one that
    # needs it: it has no expire_after, so nothing else would ever mark it
    # stale if this process dies ungracefully. The vitals keep their
    # expire_after (>= 3 poll intervals), which covers the same case for them.
    client.will_set(PROBLEM_STATE_TOPIC, "ON", qos=1, retain=True)

    delay = MQTT_CONNECT_BACKOFF
    for attempt in range(1, MQTT_CONNECT_RETRIES + 1):
        if not _running:
            log("info", "Shutdown requested during MQTT connect retry; exiting")
            raise SystemExit(0)
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=max(60, INTERVAL * 2))
            break
        except OSError as err:
            if not _running:
                log("info", "Shutdown requested during MQTT connect retry; exiting")
                raise SystemExit(0) from None
            if attempt == MQTT_CONNECT_RETRIES:
                log(
                    "fatal",
                    f"MQTT connect to {MQTT_HOST}:{MQTT_PORT} failed after "
                    f"{MQTT_CONNECT_RETRIES} attempts: {err}",
                )
                raise
            log(
                "warning",
                f"MQTT connect to {MQTT_HOST}:{MQTT_PORT} failed "
                f"(attempt {attempt}/{MQTT_CONNECT_RETRIES}): {err}; "
                f"retrying in {delay}s",
            )
            if not interruptible_sleep(delay):
                log("info", "Shutdown requested during MQTT connect retry; exiting")
                raise SystemExit(0) from None
            delay = min(delay * 2, MQTT_CONNECT_BACKOFF_MAX)

    client.loop_start()
    return client


# Aqara's own text reaches the log, the entity's cause attribute and a Markdown
# notification, so it is shown as one line of plain text, capped. The detail
# gets its own share of the cap, so a long message cannot push it out.
ERROR_TEXT_LIMIT = 200
DETAILS_LIMIT = 80
# Markdown link, image, HTML, emphasis and strikethrough syntax, and backslash
# escapes.
_MARKUP = re.compile(r"[\[\]()<>!`*~\\]")
# Strip the whole underscore run that opens or closes emphasis, so __text__
# cannot become _text_. Underscores inside identifiers stay.
_EMPHASIS_UNDERSCORE = re.compile(r"(?<!\w)_+|_+(?!\w)")


def plain_text(value):
    """`value` as one line of plain text: control and invisible format
    characters (Unicode categories C, Zl and Zp) and Markdown syntax become
    spaces, and whitespace collapses."""
    text = "".join(
        " " if unicodedata.category(char)[0] == "C" or unicodedata.category(char) in ("Zl", "Zp")
        else char
        for char in str(value)
    )
    return " ".join(_EMPHASIS_UNDERSCORE.sub(" ", _MARKUP.sub(" ", text)).split())


def code_text(code):
    """The reply's code for display. It comes from the server as well, so
    anything but an int is shown as capped plain text."""
    if isinstance(code, int) and not isinstance(code, bool):
        return str(code)[:20]
    return plain_text(code)[:20] if code is not None else "none"


def aqara_error_text(res):
    """Aqara's own error text: `message`, plus `msgDetails` when it adds
    something. `msgDetails` is where Aqara names the real cause ("Invalid
    sign"), while `message` is often a generic "Request failed"."""
    details = plain_text(res.get("msgDetails") or "")[:DETAILS_LIMIT].rstrip()
    budget = ERROR_TEXT_LIMIT - (len(details) + 3 if details else 0)
    message = plain_text(res.get("message") or "")[:budget].rstrip() or "no message returned"
    if details and details != message:
        return f"{message} ({details})"
    return message


def user_message(plain, todo, technical):
    """A message in the order a user needs it: what happened, what to do, and
    only then the technical details (code and Aqara's own text)."""
    if not technical.endswith("."):
        technical += "."
    return f"{plain} What to do: {todo} Technical details: {technical}"


def describe_poll_failure(res):
    code = res.get("code")
    if code is not None and code != 0:
        return f"Aqara API error (code={code_text(code)}): {aqara_error_text(res)}"
    # The shape only: the reply itself can carry the sensor's id and readings,
    # and this line is what users paste into a report. main() logs the raw
    # reply at debug level.
    if "result" in res:
        return (
            "unexpected response shape from Aqara API (result was a "
            f"{type(res['result']).__name__}, not a list)"
        )
    return f"unrecognized response from Aqara API (no code, {len(res)} fields)"


def is_transient_code(code):
    """True when Aqara failed to answer, false when Aqara answered "no".

    The distinction decides whether a human has to do something. A DNS blip
    clears on the next poll; a rejected password never does.
    """
    return code == -1 or code in TRANSIENT_HTTP_CODES


# What users see, by Aqara code. Every message reads: what happened, what to
# do, then the technical details.
#
#   106 at sign-in or on the sensor query -> SleepRadar's request rejected;
#                                            update SleepRadar or report it
#   108 right after a fresh sign-in       -> session ended by another sign-in
#   755 after a sign-in                   -> wrong aqara_area or subject_id
#   other code after a sign-in            -> check subject_id
#   other code at sign-in (810, ...)      -> check username, password, region
#   -1, 429, 5xx                          -> no answer from Aqara; wait
#   0 with an unreadable payload          -> unreadable reply; wait
def describe_login_failure(res, area=None, request="sign-in"):
    """Classify a failed login into (kind, cause) where kind is
    "transient" or "permanent" and cause is text a user can act on.

    `request` names the request that failed in the technical details; the
    106 message is shared with describe_failure for the sensor query."""
    code = res.get("code")
    area = area or AREA

    # Aqara answered successfully and we could not read the payload. Nothing
    # about the credentials is wrong, so this must never reach the permanent
    # branch: that would suspend polling for up to AUTH_RETRY_BACKOFF_MAX and
    # tell the user their sign-in was rejected with "code 0".
    if code == 0:
        result = res.get("result")
        shape = "no result" if result is None else f"a {type(result).__name__} result, not a list"
        return (
            "transient",
            user_message(
                "Aqara answered, but SleepRadar couldn't read the reply. This "
                "is usually temporary.",
                f"nothing yet. If it keeps happening, report it at {ISSUES_URL}.",
                # The shape only: the reply itself can carry the sensor's id and
                # readings, and this text is what users paste into a report.
                f"code 0 with {shape}",
            ),
        )

    if is_transient_code(code):
        return (
            "transient",
            user_message(
                "SleepRadar can't get an answer from Aqara right now. This is "
                "usually a short network problem or a busy Aqara server and "
                "clears on its own.",
                "nothing yet. If it doesn't clear, check that Home Assistant is "
                "online and that nothing on your network blocks Aqara's servers.",
                f"code {code_text(code)}: {aqara_error_text(res)}",
            ),
        )

    if code == AQARA_CODE_INVALID_SIGN:
        return (
            "permanent",
            user_message(
                f"SleepRadar can't get your data from Aqara: Aqara's {area} "
                "server rejected SleepRadar's request. This error does not mean "
                "your password or region is wrong.",
                "update SleepRadar first. If you already have the latest "
                f"version, report it at {ISSUES_URL}.",
                f"Aqara code {code_text(code)} on {request}: {aqara_error_text(res)}",
            ),
        )

    return (
        "permanent",
        user_message(
            "Aqara didn't accept SleepRadar's sign-in.",
            "in Settings > Apps > SleepRadar > Configuration, check "
            "aqara_username and aqara_password (your Aqara Home app account, "
            f"not the Aqara webshop account) and that aqara_area ({area}) is "
            "the region your Aqara Home app uses, then restart the app.",
            aqara_code_text(res),
        ),
    )


def aqara_code_text(res):
    """The technical-details part for a rejection: the code and Aqara's text."""
    code = res.get("code")
    if code is None:
        return f"Aqara returned no error code: {aqara_error_text(res)}"
    return f"Aqara code {code_text(code)}: {aqara_error_text(res)}"


def call_core_service(domain, service, payload):
    """Best-effort Home Assistant service call through the Supervisor proxy.

    Returns True on success. Never raises: losing a notification must not take
    the poller down, and the add-on still runs fine when homeassistant_api was
    not granted.
    """
    if not SUPERVISOR_TOKEN:
        log("debug", f"no SUPERVISOR_TOKEN; skipping {domain}.{service}")
        return False
    req = urllib.request.Request(
        f"{CORE_API}/services/{domain}/{service}",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {SUPERVISOR_TOKEN}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15):
            return True
    except Exception as err:  # noqa: BLE001 - notifications are best effort
        log("debug", f"{domain}.{service} call failed: {type(err).__name__}: {err}")
        return False


def notify_problem(cause):
    """Raise (or replace) the Home Assistant notification carrying the cause.

    A fixed notification_id means a repeat replaces the previous one instead of
    stacking a new card every poll interval.
    """
    delivered = call_core_service(
        "persistent_notification",
        "create",
        {
            "notification_id": NOTIFICATION_ID,
            "title": "SleepRadar is not receiving data",
            # The cause already says what to do. The frame adds no instruction
            # of its own: a generic "correct the options" would contradict the
            # causes the options cannot fix (an outdated SleepRadar, Aqara not
            # answering).
            "message": (
                f"{cause}\n\n"
                "Sensors stay unavailable until this is resolved.\n\n"
                "[Troubleshooting](https://github.com/florianhorner/ha-fp2-sleep"
                "#login-fails)"
            ),
        },
    )
    if not delivered:
        # Warning rather than debug: an undelivered notification is itself an
        # outage the user cannot see. Bounded, because notify_problem only runs
        # when the cause changes.
        log(
            "warning",
            "Could not raise the Home Assistant notification; the diagnostic "
            "entity still reports the problem. After updating from a version "
            "without Home Assistant API access, approve it once in the app's "
            "Configuration tab.",
        )
    return delivered


def clear_problem_notification():
    """Dismiss the problem notification once the problem is fixed."""
    return call_core_service(
        "persistent_notification", "dismiss", {"notification_id": NOTIFICATION_ID}
    )


def last_error(aqara):
    """The client's last failing login response, or {}.

    getattr because test doubles for Aqara are not required to carry it.
    """
    return getattr(aqara, "last_error", None) or {}


def describe_failure(res, login_ok, area=None):
    """(kind, cause) for whatever most recently failed.

    A rejected sign-in and a rejected device query need different advice: the
    first points at the credentials, the second at subject_id, the region, or
    a device that has left the account. Two codes keep their own meaning after
    a good sign-in: 106 is still SleepRadar's signing problem, and 108 is a
    session another sign-in ended, not a sensor-ID problem.
    """
    area = area or AREA
    code = res.get("code")
    if login_ok and code == AQARA_CODE_INVALID_SIGN:
        return describe_login_failure(res, area, request="the sensor query")
    kind, cause = describe_login_failure(res, area)
    if not login_ok or kind != "permanent":
        return kind, cause
    if code == AQARA_CODE_TOKEN_EXPIRED:
        return kind, user_message(
            "Aqara ended SleepRadar's session right after it signed in. This "
            "happens when another app or a second SleepRadar signs in with the "
            "same Aqara account.",
            "make sure only one SleepRadar (or other Aqara cloud tool) uses "
            "this account, then restart the app.",
            aqara_code_text(res),
        )
    if code == AQARA_CODE_SUBJECT_PERMISSION_DENIED:
        return kind, user_message(
            "SleepRadar signed in to Aqara but can't read your FP2: Aqara's "
            f"{area} server won't give it access to this sensor. This happens "
            "when aqara_area isn't the region your FP2 is registered in, or "
            "when subject_id points at another device.",
            "in Settings > Apps > SleepRadar > Configuration, set aqara_area to "
            "the region your Aqara Home app uses and check subject_id, then "
            f"restart the app. If both are already right, report it at {ISSUES_URL}.",
            aqara_code_text(res),
        )
    return kind, user_message(
        "SleepRadar signed in to Aqara but can't read your FP2.",
        "in Settings > Apps > SleepRadar > Configuration, check that subject_id "
        "points at your sleep FP2 and that the FP2 is still in your Aqara Home "
        "account, then restart the app.",
        aqara_code_text(res),
    )


class Health:
    """Owns the diagnostic entity and the notification that mirrors it.

    Transient failures get a grace window before they count, because a single
    DNS blip is not worth a notification. Permanent ones are flagged on the
    first occurrence: retrying will not fix them.
    """

    def __init__(self, client):
        """Track health state without publishing anything yet.

        `problem` is tri-state on purpose: None means nothing has been
        published this run, so the first result always writes, whichever way it
        goes. False and True are the two published states, and both transitions
        between them are what gates publishing and notifying.
        """
        self.client = client
        self.problem = None
        self.cause = None
        self.transient_streak = 0
        self.last_success = None

    def recovered(self):
        """Record a successful poll, clearing a problem if one was flagged."""
        self.transient_streak = 0
        self.last_success = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        if self.problem is False:
            # Steady-state healthy: publish nothing. last_success is tracked in
            # memory and shipped with the next failure, which is the only time
            # anyone asks for it. Republishing it every poll would write a
            # retained attribute update per interval, 1440 a day, on an entity
            # this repo tells people to put in Recorder.
            return
        if self.problem:
            log("info", "Aqara connection recovered; clearing the problem flag")
        publish_problem(self.client, False, last_success=self.last_success)
        clear_problem_notification()
        self.problem = False
        self.cause = None

    def failed(self, kind, cause, code):
        """Record a failed poll, flagging a problem once it counts.

        Transient failures only count after TRANSIENT_FAILURE_GRACE of them in
        a row; permanent ones count immediately.
        """
        if kind == "transient":
            self.transient_streak += 1
            if self.transient_streak < TRANSIENT_FAILURE_GRACE:
                log(
                    "debug",
                    f"transient failure {self.transient_streak}/"
                    f"{TRANSIENT_FAILURE_GRACE} before flagging a problem",
                )
                return
        else:
            self.transient_streak = 0

        # Re-notify when the *reason* changes, not just on the first failure.
        # A blip that turns out to be a permanent rejection would otherwise
        # leave the notification reading "clears on its own" for the rest of
        # the outage. An unchanged cause stays silent, so an ongoing outage
        # does not re-notify every poll.
        changed = self.problem is not True or cause != self.cause
        if not changed:
            # Nothing in the payload differs, and it is retained, so the broker
            # is already serving the current state. Republishing it every poll
            # would write a Recorder row per interval for the whole outage.
            return
        publish_problem(
            self.client, True, cause=cause, code=code, last_success=self.last_success
        )
        log("error", f"Flagging a connection problem: {cause}")
        notify_problem(cause)
        self.problem = True
        self.cause = cause


_running = True


def _stop(*_):
    global _running
    _running = False


def interruptible_sleep(seconds):
    for _ in range(seconds):
        if not _running:
            return False
        time.sleep(1)
    return _running


# Cooldown before exiting on a permanent startup misconfiguration. If the process
# is supervised by an external watchdog, this keeps blank/typo'd config or an
# unknown area from becoming a tight restart loop that burns CPU and floods logs.
STARTUP_FAILURE_COOLDOWN = int(os.getenv("STARTUP_FAILURE_COOLDOWN", "30"))


def fatal_startup(msg):
    log("fatal", msg)
    interruptible_sleep(STARTUP_FAILURE_COOLDOWN)
    raise SystemExit(1)


def main():
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    if not (USER and PASSWORD and SUBJECT):
        fatal_startup("Missing AQARA_USER / AQARA_PASS / SUBJECT_ID")

    client = make_mqtt()
    publish_discovery(client)

    aqara = Aqara(AREA)
    health = Health(client)

    login_ok = aqara.login()
    if not login_ok:
        startup_error = last_error(aqara)
        kind, cause = describe_login_failure(startup_error)
        # Not "fatal": this process keeps running and recovers on its own once
        # the cause is gone (an options change needs an app restart). Calling
        # it fatal told users to expect a crash that never came.
        # The cause goes last so its technical details end the line.
        log(
            "error",
            "Aqara login failed at startup; SleepRadar keeps retrying, and the "
            f"sensors stay unavailable until the sign-in succeeds. {cause}",
        )
        # Flag it now rather than after the first poll. A rejected sign-in at
        # boot is already certain, so waiting a full poll interval to say so
        # would leave the user without an answer for no reason. Transient
        # failures still go through the grace window inside Health.
        health.failed(kind, cause, startup_error.get("code"))

    auth_wait = 0
    auth_delay = AUTH_RETRY_BACKOFF
    # The reply's field names and measurement ages are logged once per run,
    # so users can see what Aqara actually sends without debug logging.
    reply_fields_logged = False
    # The FP2's (mode, offline) codes, logged whenever they change.
    last_device_status = None
    # The history log is checked once per run, after the first good poll.
    history_checked = False
    # Tonight's stage changes, for the night totals.
    night = NightStages()

    while _running:
        # Aqara has rejected these credentials. Retrying at the poll interval
        # would hammer the auth endpoint for no possible gain, so hold off.
        if auth_wait > 0:
            log("debug", f"auth backoff: {auth_wait}s before retrying the sign-in")
            auth_wait = max(0, auth_wait - INTERVAL)
            interruptible_sleep(INTERVAL)
            continue

        ok = False
        res = aqara.res_query(SUBJECT, SLEEP_OPTIONS)
        code = res.get("code")
        if code != 0 and is_transient_code(code) and has_session(aqara):
            # Aqara did not answer (network, timeout, 429 or 5xx), so the
            # session says nothing about it. Signing in again would add load
            # to a server that is already failing or rate-limiting, and each
            # new sign-in can end another client's session on the same
            # account. Keep the session; Health counts this as transient.
            log(
                "warning",
                f"res/query code={code_text(code)} ({aqara_error_text(res)}); "
                "Aqara did not answer, keeping the session",
            )
        elif code != 0:
            log(
                "warning",
                f"res/query code={code_text(code)} ({aqara_error_text(res)}); re-login",
            )
            login_ok = aqara.login()
            if login_ok:
                res = aqara.res_query(SUBJECT, SLEEP_OPTIONS)
            else:
                res = last_error(aqara) or res

        if res.get("code") == 0 and isinstance(res.get("result"), list):
            state, mode, offline = build_state(res["result"])
            apply_night_totals(state, night, aqara)
            client.publish(STATE_TOPIC, json.dumps(state), qos=0, retain=False)
            client.publish(AVAIL_TOPIC, "online", qos=1, retain=True)
            ok = True
            if not reply_fields_logged:
                log("info", describe_reply_fields(res["result"]))
                reply_fields_logged = True
            if (mode, offline) != last_device_status:
                level, message = device_status_message(mode, offline)
                log(level, message)
                last_device_status = (mode, offline)
            log("debug", f"state={state}")
            if not history_checked:
                history_checked = True
                read_fp2_firmware(aqara, client)
                probe_device_lists(aqara)
                run_history_check(aqara)

        if ok:
            auth_delay = AUTH_RETRY_BACKOFF
            health.recovered()
        else:
            client.publish(AVAIL_TOPIC, "offline", qos=1, retain=True)
            log("error", f"poll failed: {describe_poll_failure(res)}")
            log("debug", f"raw Aqara reply: {json.dumps(res)[:ERROR_TEXT_LIMIT]}")
            kind, cause = describe_failure(res, login_ok)
            health.failed(kind, cause, res.get("code"))
            if kind == "permanent":
                # The end-of-loop sleep below already burns one interval, so
                # subtract it here. Without this the real gap between sign-in
                # attempts is auth_delay + INTERVAL and the logged countdown
                # understates the actual wait.
                auth_wait = max(0, auth_delay - INTERVAL)
                auth_delay = min(auth_delay * 2, AUTH_RETRY_BACKOFF_MAX)

        interruptible_sleep(INTERVAL)

    # A requested stop is not a problem, and a problem that was already flagged
    # does not stop being one. Either way the retained diagnostic state is left
    # as it is; the MQTT will fires only on an ungraceful death, which is the
    # case that needs flagging.
    log("info", "Shutting down; marking offline.")
    client.publish(AVAIL_TOPIC, "offline", qos=1, retain=True)
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
