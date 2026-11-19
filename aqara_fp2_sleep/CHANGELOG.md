# Changelog

## 1.4.2

- A one-time startup check logs which Aqara calls list your devices (field
  names and models only, no IDs).

## 1.4.1

- The device shows the FP2's own firmware, and "via SleepRadar" shows the
  real app version.

## 1.4.0

- New night totals: hours asleep, light, deep and REM since 18:00, from
  Aqara's own timestamps (`sensor.aqara_fp2_sleep_night_*`). Exact by
  morning, unlike time-in-state counts on the delayed live sleep state.

## 1.3.7

- The history check lists every sleep-stage change of the last 14 hours.

## 1.3.6

- The history check also scans heart rate hour by hour through one past
  night, to show how often the FP2 updates it.

## 1.3.5

- The history check also samples an hour of each of the past seven nights
  (`History check, hour ending …` lines), to tell a sensor that never locks
  onto heart rate and breathing from a bad night.

## 1.3.4

- Once per start, the log reports what Aqara's history log holds for the last
  two hours of heart rate, breathing, sleep state and body movement
  (`History check:` lines). Diagnostic only; nothing new is published.

## 1.3.3

- `binary_sensor.aqara_fp2_sleep_fp2_online` no longer shows an online FP2 as
  offline: Aqara's `device_offline_status` reads `1` while it is online.

- The card's "Measured … ago" follows the newest of sleep state, heart rate
  and breathing, so a sleep stage held for hours no longer makes live vitals
  look old.

## 1.3.2

Readings now carry the time Aqara measured them, Aqara outages no longer
trigger a new sign-in, and the FP2's mode and online state are visible.

- Each vital sensor has a `measured_at` attribute with Aqara's own timestamp
  for that reading. The SleepRadar Card shows "Measured … ago" from it instead
  of "Updated just now", which only meant the app had polled.

- When Aqara does not answer (network error, timeout, `429` or `5xx`), the
  app keeps its session and retries on the next poll instead of signing in
  again. A rejected or missing session still signs in again.

- New diagnostic entities `sensor.aqara_fp2_sleep_fp2_mode` and
  `binary_sensor.aqara_fp2_sleep_fp2_online`. The log warns when the FP2 is
  offline or not in Sleep monitoring mode.

- The log names the fields Aqara sends and each measurement's age once per
  start, without the sensor's id.

- A reading entry Aqara sends in an unexpected shape is skipped instead of
  stopping the app.

## 1.3.1

Removes the wrong-key error that blocked USA and China sign-in. Connection
messages now explain the next step and include Aqara's diagnostic detail.

Thanks to @awk-git for reporting the USA sign-in failure and comparing the
regional behavior in [#40](https://github.com/florianhorner/ha-fp2-sleep/issues/40).

- Corrects the built-in signing keys for `aqara_area: USA` and `CN`, which
  caused Aqara to reject requests with code `106`.

- Error messages say what happened, then what to do, then the technical
  details. Code `106` no longer points at `aqara_area`.

- The notification no longer tells you to change options that cannot fix the
  cause, and the log includes Aqara's detail text as plain text. Markdown
  emphasis is removed while diagnostic identifiers stay readable.

- Gateway and proxy errors retain their HTTP status when the reply has no
  usable Aqara code, including JSON strings, lists, and `null`. These errors
  count as Aqara not answering and no longer stop polling on non-object JSON.
  The same applies to a successful reply that is not a JSON object and to a
  sign-in reply without a session.

- Signing in again clears the previous session's token from the request.

### Verification

Verified against Aqara's US service using a dedicated USA-region account.
The original signing configuration reproduced code `106`; the corrected
configuration signed in successfully with code `0` and a session token.
US FP2 sleep-data retrieval has not yet been verified. China's evidence covers
request-signature acceptance; real China-account sign-in remains unverified.

### Upgrade

Update SleepRadar to 1.3.1. If you changed `aqara_area` away from your account's
USA or China region because of code `106`, set it back to `USA` or `CN` and
restart the SleepRadar app.

## 1.3.0

- Adds `binary_sensor.aqara_fp2_sleep_connection_problem`, a diagnostic entity
  that turns on when data stops and names the reason in its `cause` attribute.
  It stays readable while the five sensors are unavailable.

- Raises a Home Assistant notification with the cause and the fix when the
  Aqara connection fails, updates it if the cause changes, and dismisses it on
  recovery. Needs the new `homeassistant_api: true` permission, used for that
  call only. **Approve it once after updating**; until then the diagnostic
  entity still works and the notification is skipped with a `warning` in the
  log.

- Reports why a sign-in was rejected instead of repeating Aqara's "Request
  failed. Please try again." Code `106` now points at `aqara_area`.

- Backs off between retries after Aqara rejects the credentials, instead of
  re-attempting the sign-in every poll interval.

- Logs a failed startup sign-in at `error` instead of `fatal`. The app keeps
  running and recovers once the options are corrected.

- **Breaking for existing `examples/` template users.** The optional helpers now
  require an independent `binary_sensor.bed_occupied` and go unavailable without
  it, and the deep-sleep automation trigger is renamed to
  `"Deep sleep (indicative)"`. Update existing copies or they stop firing. The
  app's own MQTT entities and polling are unchanged.

- Documents that the `sleep_state` 0-5 mapping is community-derived and
  unverified. With an independent occupancy gate active, codes `0`/`1`/`2` no
  longer assert out-of-bed or awake state. No-gate behavior keeps its legacy
  labels for compatibility.

- Covers ghost vitals in a synthetic regression fixture with authored readings for
  codes `3`/`4`/`5`, changing values and repeated samples while independent
  occupancy stays empty. It checks that plausible readings cannot bypass the
  card's occupancy gate; it contains no recorded sensor data.

- Adds an optional, fail-closed `bed_occupancy` gate to the SleepRadar Card.
  It hides live vitals when independent occupancy is empty or uncertain while
  leaving the app's MQTT telemetry and Recorder history unchanged. It rejects
  direct reuse of a card data entity and trusts Home Assistant availability
  without adding an occupancy-age timeout.

## 1.2.2

- Removes the invalid boolean `watchdog` add-on config and adds a validator
  guard so `watchdog`, if ever used, must be a health-check URL string instead
  of a restart-toggle boolean.
- Permanent startup failures now slow-exit before returning an error: the
  wrapper waits ~30s for missing MQTT service or required Aqara config, and the
  Python poller does the same for direct-run missing config or unknown
  `aqara_area`. This prevents rapid loops when an external Supervisor watchdog
  is enabled.
- Logs a clear `[fatal]` line when the initial Aqara login fails, instead of
  only per-poll warnings. The add-on keeps retrying and the sensors stay
  unavailable until login succeeds.

## 1.2.1

- Fixes the SleepRadar Card's out-of-bed state so retained FP2 heart-rate and
  breathing values are hidden instead of looking live after the bed is empty.
  The card now labels freshness in the header, shows per-vital status text,
  and only renders live vitals when the sleep state is fresh and in-bed.

## 1.2.0

- Adds `card/sleepradar-card.js`, a dependency-free custom Lovelace card that
  renders the "Now, live" README view (phase, heart rate, breathing) from
  three of the add-on's default sensors, with honest unavailable/stale
  states. Install is a manual dashboard resource. The "Last night"
  screenshot is relabeled as a preview of a future release, since the
  optional examples dashboard does not actually reproduce it (see below).
  The card sanitizes a custom `mqtt_node_id` the same way the add-on does,
  and its `poll_interval_seconds` option keeps the "stale" badge accurate
  if you changed the add-on's `poll_interval` from the 60-second default.
- Adds `force_update: true` to the add-on's MQTT discovery payloads. Without
  it, Home Assistant does not advance an entity's `last_updated` when a poll
  republishes an unchanged value (e.g. a stable deep-sleep reading), which
  would have made the SleepRadar Card's "stale" badge false-positive during
  normal operation.
- `scripts/validate_repository.py` now checks that the card's default
  entities are a subset of what the add-on actually publishes, and that
  every discovery payload sets `force_update: true`.
- The card now treats a `sleep_state` of `"None"`/`"none"` as unavailable.
  The poller's `value_template` only guards Jinja's `Undefined`, not a
  literal `null`, so a null Aqara reading renders as the literal text
  "None" in Home Assistant — without this, the card showed a fabricated
  "Unknown" live reading instead of the honest "no data yet" state.
- Corrects the README's "Last night" claim: `examples/dashboard-sleep.yaml`
  only produces a live "Now" card plus a raw 12-hour sleep-state chart, not
  the session duration, averaged vitals, or segmented stage timeline shown
  in the screenshot — that sessionization is still a future release, not
  something buildable today from the example.

## 1.1.0

- Adds `default_entity_id` to MQTT discovery payloads alongside the existing
  `object_id`, so entity IDs are pinned correctly on Home Assistant ≥2026.4
  where `object_id` was removed from the discovery schema.
- Adds SleepRadar design language guidance and logo assets for public docs.
- Adds an upfront README disclosure that the add-on relies on Aqara's private,
  unofficial Home app API, which Aqara can change or restrict without notice.
- Pre-wires `examples/sleep_tracking.yaml` and `examples/dashboard-sleep.yaml`
  to the add-on's own default entity IDs so both load with no editing beyond
  the dashboard's optional bed-status cross-check card.
- Aligns the root README with the pre-wired examples, documents that
  `sleep_tracking.yaml` must load before the dashboard, and widens the
  example validator's foreign-entity guard from `sensor.` to all common
  entity domains (service-call lines excluded).
- Adds a one-click "Add repository to my Home Assistant" install badge and a
  CI status badge to the README, and documents the full `aqara_area` region
  list (`CN`, `EU`, `USA`, `RU`, `KR`) inline instead of only in the config
  schema.
- Poll failures now log a plain-English cause (the Aqara API error code and
  message, or a clear description of an unexpected response) instead of a
  raw JSON dump.
- The add-on now retries the MQTT broker connection with backoff (up to
  ~2 minutes) instead of exiting immediately if the broker is still starting
  up — common right after a fresh install when both add-ons boot together —
  and still responds promptly to add-on stop/restart during that retry
  window.

## 1.0.0

- Initial public package for Aqara FP2 sleep telemetry in Home Assistant.
- Adds a Home Assistant add-on that polls Aqara sleep resources and publishes
  five MQTT discovery sensors.
- Adds plain-language setup docs, placeholder-only examples, and a privacy
  validator for release checks.
