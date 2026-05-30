function safeJsonParse(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch (_) {
    localStorage.removeItem(key);
    return fallback;
  }
}

function safeJsonArray(key) {
  const value = safeJsonParse(key, []);
  return Array.isArray(value) ? value : [];
}

function safeJsonObject(key) {
  const value = safeJsonParse(key, {});
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

const APP_VERSION = "20260530-stage4-voice-a-1";
const APP_SCRIPT = "app.js?v=20260530-stage4-voice-a-1";
const EXPECTED_SW_CACHE = "hira-os-v141";
const CHAT_DEBUG_TRACE = localStorage.getItem("hira_pwa_debug_trace") === "1";
const INTERNAL_TOOL_FALLBACK = "I caught an internal tool note instead of a proper reply, so I hid it from the chat. Try that once more.";
const HOME_CACHE_KEY = "hira_pwa_home_snapshot_v1";
const RIGHT_NOW_CACHE_KEY = "hira_pwa_right_now_snapshot_v1";
const AGENDA_CACHE_KEY = "hira_pwa_agenda_snapshot_v1";
const PUSH_SYNC_MODE_KEY = "hira_pwa_last_push_sync_mode";
const PUSH_SYNC_ENDPOINT_KEY = "hira_pwa_last_push_sync_endpoint";
const SESSION_TOKEN_KEY = "hira_web_session_token";
const AUTO_SPEAK_KEY = "hira_pwa_auto_speak_replies";
const HOME_CACHE_MAX_AGE_MS = 6 * 60 * 60 * 1000;
const HOME_REFRESH_THROTTLE_MS = 45 * 1000;
const RIGHT_NOW_REFRESH_MS = 60 * 1000;
const SOURCE_PLUMBING_URL_PATTERN = /https?:\/\/(?:news\.google\.com\/rss\/articles|site\.api\.espn\.com\/apis\/|duckduckgo\.com\/l\/\?)\S+/gi;
let legacyWebToken = localStorage.getItem("hira_web_token") || "";
let runtimeWebToken = "";
try {
  runtimeWebToken = sessionStorage.getItem(SESSION_TOKEN_KEY) || "";
} catch (_) {
  runtimeWebToken = "";
}
const persistedWebToken = legacyWebToken || runtimeWebToken;

const state = {
  token: persistedWebToken,
  sessionUnlocked: localStorage.getItem("hira_session_unlocked") === "1" || Boolean(persistedWebToken),
  theme: localStorage.getItem("hira_theme") || "light",
  clientId: localStorage.getItem("hira_client_id") || (globalThis.crypto?.randomUUID ? globalThis.crypto.randomUUID() : `hira-${Date.now()}`),
  deferredInstall: null,
  currentView: "home",
  homeDays: 7,
  chatBusy: false,
  chatAttachments: [],
  chatHistory: safeJsonArray("hira_pwa_chat"),
  notifications: safeJsonArray("hira_pwa_notifications"),
  dismissedNotificationIds: safeJsonArray("hira_pwa_dismissed_notification_ids"),
  dismissedHomeSections: safeJsonArray("hira_pwa_dismissed_home_sections"),
  chatNotificationIds: safeJsonArray("hira_pwa_chat_notification_ids"),
  feedback: safeJsonObject("hira_pwa_feedback"),
  deviceLocation: safeJsonParse("hira_pwa_device_location", null),
  notificationPoll: null,
  activeNotificationId: "",
  activeNotificationItem: null,
  lastPushSyncAt: Number(localStorage.getItem("hira_pwa_last_push_sync_at") || "0"),
  lastPushSyncMode: localStorage.getItem(PUSH_SYNC_MODE_KEY) || "",
  lastPushSyncEndpoint: localStorage.getItem(PUSH_SYNC_ENDPOINT_KEY) || "",
  lastInputPulseAt: 0,
  homeRefreshInFlight: null,
  homeLastRefreshStartedAt: 0,
  homeTimelineItems: [],
  rightNow: null,
  rightNowSavedAt: 0,
  rightNowReceivedAt: 0,
  rightNowRefreshInFlight: null,
  rightNowPoll: null,
  autoSpeak: localStorage.getItem(AUTO_SPEAK_KEY) === "1",
  voiceRecorder: null,
  voiceStream: null,
  voiceChunks: [],
  voiceRecording: false,
  speechAudio: null,
  speechUrl: "",
  actionLedger: [],
};
state.chatHistory = cleanStoredChatHistory(state.chatHistory);
saveChatHistory();

const $ = (selector) => document.querySelector(selector);
const urlTheme = new URLSearchParams(window.location.search).get("theme");
const clockFormatter = new Intl.DateTimeFormat("en-SG", {
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
  timeZone: "Asia/Singapore",
});
const dateFormatter = new Intl.DateTimeFormat("en-SG", {
  weekday: "long",
  day: "2-digit",
  month: "long",
  timeZone: "Asia/Singapore",
});
const glyphDigitFont = {
  "0": ["111", "101", "101", "101", "111"],
  "1": ["010", "110", "010", "010", "111"],
  "2": ["111", "001", "111", "100", "111"],
  "3": ["111", "001", "111", "001", "111"],
  "4": ["101", "101", "111", "001", "001"],
  "5": ["111", "100", "111", "001", "111"],
  "6": ["111", "100", "111", "101", "111"],
  "7": ["111", "001", "010", "010", "010"],
  "8": ["111", "101", "111", "101", "111"],
  "9": ["111", "101", "111", "001", "111"],
  ":": ["0", "1", "0", "1", "0"],
  "%": ["101", "001", "010", "100", "101"],
  "-": ["000", "000", "111", "000", "000"],
};
const GLYPH_COLS = 21;
const GLYPH_ROWS = 13;
const GLYPH_MODES = ["time", "date", "battery", "load", "next"];
let glyphMode = "time";
let glyphModeBeforeChat = "time";
let batteryPercent = null;
let homeGlyphDataReady = false;
const CONNECTIONS = [
  { key: "calendar", label: "Calendar", icon: "calendar" },
  { key: "google", label: "Google", icon: "sparkles" },
  { key: "work_drive", label: "Work Google Drive", icon: "folder" },
  { key: "personal_gmail", label: "Personal Gmail", icon: "mail" },
  { key: "personal_gmail2", label: "Personal Gmail 2", icon: "mail-plus" },
  { key: "work_gmail", label: "Work Gmail", icon: "briefcase-business" },
];
const COMMAND_STATUS = {
  send: "Command launched.",
  fill: "Command staged. Add the missing detail and send.",
};

function updateLiveClock() {
  const now = new Date();
  const time = clockFormatter.format(now).replace(/^24:/, "00:");
  const date = dateFormatter.format(now).replace(",", "").toUpperCase();
  $("#greetingTime").textContent = time;
  $("#greetingDate").textContent = date;
  if (glyphMode === "time" || glyphMode === "next") renderNothingGlyph(glyphMode);
}

function blankGlyphGrid() {
  return Array.from({ length: GLYPH_ROWS }, () => Array.from({ length: GLYPH_COLS }, () => ""));
}

function drawGlyphPixel(grid, x, y, tone = "on") {
  if (!grid[y] || x < 0 || x >= grid[y].length) return;
  grid[y][x] = tone;
}

function drawGlyphText(grid, value, startX, startY, tone = "on") {
  let cursor = startX;
  for (const char of String(value || "")) {
    const glyph = glyphDigitFont[char];
    if (!glyph) {
      cursor += 2;
      continue;
    }
    glyph.forEach((row, y) => {
      [...row].forEach((cell, x) => {
        if (cell === "1") drawGlyphPixel(grid, cursor + x, startY + y, tone);
      });
    });
    cursor += glyph[0].length + 1;
  }
}

function glyphTextWidth(value) {
  return [...String(value || "")].reduce((total, char) => total + (glyphDigitFont[char]?.[0].length || 1) + 1, -1);
}

function drawGlyphTextCentered(grid, value, startY, tone = "on") {
  const width = glyphTextWidth(value);
  drawGlyphText(grid, value, Math.max(0, Math.floor((GLYPH_COLS - width) / 2)), startY, tone);
}

function drawGlyphDivider(grid, y = 6) {
  [6, 7, 9, 10, 11, 13, 14].forEach((x) => drawGlyphPixel(grid, x, y, "dim"));
}

function drawGlyphMeter(grid, percent) {
  const clean = Number.isFinite(percent) ? Math.max(0, Math.min(100, percent)) : 0;
  const filled = Math.round((clean / 100) * 11);
  for (let x = 5; x <= 15; x += 1) drawGlyphPixel(grid, x, 11, x - 5 < filled ? "hot" : "dim");
}

function drawGlyphCompactClock(grid, value, topY = 3) {
  const clean = String(value || "--:--").padStart(5, "-").slice(0, 5);
  const positions = [2, 6, 10, 12, 16];
  [...clean].forEach((char, index) => drawGlyphText(grid, char, positions[index], topY, "on"));
}

function drawGlyphFooter(grid, footer) {
  const marks = {
    MAY: [[5, 10], [6, 10], [8, 10], [10, 10], [5, 11], [7, 11], [8, 11], [10, 11]],
    NEW: [[5, 10], [5, 11], [7, 10], [8, 10], [7, 11], [10, 10], [10, 11], [11, 11]],
    NXT: [[4, 10], [4, 11], [5, 10], [6, 11], [8, 10], [10, 10], [9, 11], [13, 10], [14, 10], [15, 10], [14, 11]],
  }[footer] || [];
  marks.forEach(([x, y]) => drawGlyphPixel(grid, x, y, "dim"));
}

function drawGlyphWave(grid) {
  const center = 6;
  const heights = [0, 0, 1, 1, 2, 4, 5, 2, 1, 3, 4, 2, 3, 2, 1, 2, 1, 1, 0, 0, 0];
  heights.forEach((height, index) => {
    drawGlyphPixel(grid, index, center, "hot");
    for (let n = 1; n <= height; n += 1) {
      drawGlyphPixel(grid, index, center - n, n > 3 ? "hot" : "on");
      drawGlyphPixel(grid, index, center + n, n > 3 ? "hot" : "on");
    }
  });
  for (let x = 0; x < GLYPH_COLS; x += 1) drawGlyphPixel(grid, x, center, "hot");
  [3, 7, 10, 13, 17].forEach((x) => {
    drawGlyphPixel(grid, x, center - 1, "dim");
    drawGlyphPixel(grid, x, center + 1, "dim");
  });
}

function glyphValueForMode(mode) {
  const now = new Date();
  if (mode === "time") return { value: clockFormatter.format(now).replace(/^24:/, "00:"), label: "local time" };
  if (mode === "date") {
    const day = new Intl.DateTimeFormat("en-SG", { day: "2-digit", timeZone: "Asia/Singapore" }).format(now);
    const month = new Intl.DateTimeFormat("en-SG", { month: "2-digit", timeZone: "Asia/Singapore" }).format(now);
    return { value: `${day}-${month}`, label: "date" };
  }
  if (mode === "battery") {
    const value = Number.isFinite(batteryPercent) ? Math.round(batteryPercent) : null;
    return { value: value === null ? "--%" : `${String(value).padStart(2, "0")}%`, percent: value, label: "battery" };
  }
  if (mode === "mail") return { value: String(Math.min(99, state.notifications.length || 0)).padStart(2, "0"), footer: "NEW", label: "mail notifications" };
  if (mode === "load") {
    if (!homeGlyphDataReady) return { value: "--%", percent: null, label: "workload loading" };
    const score = Number($("#dailyLoadScore")?.textContent || 0);
    const value = Math.max(0, Math.min(99, Math.round(score)));
    return { value: `${String(value).padStart(2, "0")}%`, percent: value, label: "workload" };
  }
  if (mode === "next") {
    if (!homeGlyphDataReady) return { value: "--:--", label: "next anchor loading" };
    const now = new Date();
    const currentMinutes = now.getHours() * 60 + now.getMinutes();
    const items = Array.isArray(state.homeTimelineItems) ? state.homeTimelineItems : [];
    const nextItem = items.find((item) => {
      if (!Number.isFinite(item.start)) return false;
      const end = Number.isFinite(item.end) ? item.end : item.start + 20;
      return item.start >= currentMinutes || end >= currentMinutes;
    });
    const nextTime = String(nextItem?.time || "").match(/\b\d{1,2}:\d{2}\b/)?.[0];
    return { value: nextTime || "--:--", label: nextItem ? "next anchor" : "no upcoming anchor" };
  }
  return { value: "", label: mode };
}

function drawGlyphMode(grid, mode, current) {
  if (mode === "time") {
    drawGlyphCompactClock(grid, current.value, 3);
    return;
  }
  if (mode === "date") {
    drawGlyphTextCentered(grid, current.value, 3);
    return;
  }
  if (mode === "battery" || mode === "load") {
    drawGlyphTextCentered(grid, current.value, 2);
    drawGlyphMeter(grid, current.percent);
    return;
  }
  if (mode === "next") {
    drawGlyphCompactClock(grid, current.value, 3);
    drawGlyphFooter(grid, "NXT");
    return;
  }
  drawGlyphTextCentered(grid, current.value, 3);
  if (current.footer) drawGlyphFooter(grid, current.footer);
}

function renderNothingGlyph(mode = glyphMode) {
  const button = $("#nothingGlyphBtn");
  const matrix = $("#nothingGlyphMatrix");
  if (!button || !matrix) return;
  glyphMode = mode;
  const grid = blankGlyphGrid();
  button.dataset.glyphMode = mode;
  if (mode === "chat") {
    drawGlyphWave(grid);
    button.dataset.glyphLabel = "CHAT";
    button.setAttribute("aria-label", "Glyph display showing H.I.R.A chat waveform.");
  } else {
    const current = glyphValueForMode(mode);
    drawGlyphMode(grid, mode, current);
    button.dataset.glyphLabel = ({
      time: "TIME",
      date: "DATE",
      battery: "BATT",
      load: "LOAD",
      next: "NEXT",
    }[mode] || String(current.label || mode).toUpperCase().slice(0, 5));
    button.setAttribute("aria-label", `Glyph display showing ${current.label}: ${current.value}. Tap to cycle mode.`);
  }
  matrix.innerHTML = grid
    .flatMap((row) => row.map((tone) => `<span class="nothing-glyph-dot ${tone}"></span>`))
    .join("");
}

function cycleNothingGlyph() {
  const currentIndex = Math.max(0, GLYPH_MODES.indexOf(glyphMode));
  renderNothingGlyph(GLYPH_MODES[(currentIndex + 1) % GLYPH_MODES.length]);
}

async function initBatteryGlyph() {
  if (!("getBattery" in navigator)) return;
  try {
    const battery = await navigator.getBattery();
    const update = () => {
      batteryPercent = Math.round(Number(battery.level || 0) * 100);
      if (glyphMode === "battery") renderNothingGlyph("battery");
    };
    update();
    battery.addEventListener("levelchange", update);
  } catch {
    batteryPercent = null;
  }
}

function resolvedTheme() {
  if (urlTheme === "light" || urlTheme === "dark") return urlTheme;
  if (state.theme === "auto") {
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  return state.theme;
}

function applyTheme() {
  const theme = resolvedTheme();
  document.documentElement.dataset.theme = theme;
  const themeColor = theme === "dark" ? "#000000" : "#e4e3df";
  document.querySelector('meta[name="theme-color"]')?.setAttribute("content", themeColor);
  document.querySelectorAll("[data-theme-choice], .theme-btn").forEach((btn) => {
    const selected = urlTheme ? btn.dataset.theme === urlTheme : btn.dataset.theme === state.theme;
    btn.classList.toggle("active", selected);
  });
}

function headers(json = true) {
  const base = { "X-Hira-CSRF": "1" };
  if (json) base["Content-Type"] = "application/json";
  if (state.token) base["X-Hira-Token"] = state.token;
  if (state.clientId) base["X-Hira-Client"] = state.clientId;
  return base;
}

function withAuth(options = {}) {
  const next = { ...options, headers: { ...(options.headers || {}) } };
  next.headers["X-Hira-CSRF"] = "1";
  if (state.token) next.headers["X-Hira-Token"] = state.token;
  if (state.clientId) next.headers["X-Hira-Client"] = state.clientId;
  return next;
}

function openTokenSettings(message = "Save the H.I.R.A web token in Settings to sync live data.") {
  const panel = $("#settingsPanel");
  if (panel) panel.hidden = false;
  $("#settingsBtn")?.classList.add("is-open");
  $("#tokenInput")?.focus();
  setStatus(message, "warn");
}

async function createSession(token) {
  const clean = String(token || "").trim();
  if (!clean) throw new Error("Paste the H.I.R.A web token first.");
  const response = await fetch("/api/auth/session", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(state.clientId ? { "X-Hira-Client": state.clientId } : {}),
    },
    body: JSON.stringify({ token: clean }),
  });
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.detail || `Session unlock failed: ${response.status}`);
  }
  state.token = clean;
  try {
    sessionStorage.setItem(SESSION_TOKEN_KEY, clean);
  } catch (_) {
    // Session storage may be unavailable in some installed-app contexts.
  }
  state.sessionUnlocked = true;
  localStorage.setItem("hira_session_unlocked", "1");
  state.lastPushSyncAt = 0;
  state.lastPushSyncMode = "";
  state.lastPushSyncEndpoint = "";
  localStorage.removeItem("hira_pwa_last_push_sync_at");
  localStorage.removeItem(PUSH_SYNC_MODE_KEY);
  localStorage.removeItem(PUSH_SYNC_ENDPOINT_KEY);
  const tokenInput = $("#tokenInput");
  if (tokenInput) tokenInput.value = "";
  return response.json().catch(() => ({ ok: true }));
}

async function migrateLegacyToken() {
  const token = String(legacyWebToken || runtimeWebToken || "").trim();
  legacyWebToken = "";
  if (!token) return;
  state.token = token;
  state.sessionUnlocked = true;
  localStorage.removeItem("hira_web_token");
  localStorage.setItem("hira_session_unlocked", "1");
  try {
    sessionStorage.setItem(SESSION_TOKEN_KEY, token);
  } catch (_) {
    // Keep localStorage as the durable installed-app fallback.
  }
}

async function api(path, options = {}, tokenPrompted = false) {
  const {
    retryNetwork = false,
    retryLabel = "request",
    ...fetchOptions
  } = options || {};
  let response;
  let networkError = null;
  const maxAttempts = retryNetwork ? 4 : 1;
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    try {
      response = await fetch(path, withAuth(fetchOptions));
      networkError = null;
      break;
    } catch (error) {
      networkError = error;
      if (attempt >= maxAttempts - 1) break;
      const delay = 900 + attempt * 1400;
      setStatus(`Connection dropped during ${retryLabel}. Retrying...`, "warn");
      await wait(delay);
    }
  }
  if (!response) {
    throw new Error(
      retryNetwork
        ? `Could not reach H.I.R.A during ${retryLabel}. The upload was not abandoned; try again on a stable connection.`
        : (networkError?.message || "Network request failed.")
    );
  }
  if (response.status === 401) {
    state.token = "";
    state.sessionUnlocked = false;
    localStorage.removeItem("hira_web_token");
    try {
      sessionStorage.removeItem(SESSION_TOKEN_KEY);
    } catch (_) {}
    localStorage.removeItem("hira_session_unlocked");
    openTokenSettings(
      tokenPrompted
        ? "That token was rejected. Paste the H.I.R.A web token in Settings and save it once."
        : "Session expired. Save the H.I.R.A web token in Settings to sync live data."
    );
    throw new Error(
      tokenPrompted
        ? "That token was rejected. Paste the H.I.R.A web token in Settings and save it once."
        : "H.I.R.A needs the web token before live data can load."
    );
  }
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.detail || `Request failed: ${response.status}`);
  }
  return response.json();
}

async function fetchWithToken(path, options = {}, tokenPrompted = false) {
  const response = await fetch(path, withAuth(options));
  if (response.status === 401) {
    state.token = "";
    state.sessionUnlocked = false;
    localStorage.removeItem("hira_web_token");
    try {
      sessionStorage.removeItem(SESSION_TOKEN_KEY);
    } catch (_) {}
    localStorage.removeItem("hira_session_unlocked");
    openTokenSettings(
      tokenPrompted
        ? "That token was rejected. Paste the H.I.R.A web token in Settings and save it once."
        : "Session expired. Save the H.I.R.A web token in Settings to sync live data."
    );
    throw new Error(
      tokenPrompted
        ? "That token was rejected. Paste the H.I.R.A web token in Settings and save it once."
        : "H.I.R.A needs the web token before live data can load."
    );
  }
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}));
    throw new Error(detail.detail || `Request failed: ${response.status}`);
  }
  return response;
}

function refreshIcons(root = document) {
  window.lucide?.createIcons({
    attrs: { "stroke-width": 1.8 },
    nameAttr: "data-lucide",
    root,
  });
}

function setStatus(text, tone = "muted") {
  const el = $("#statusLine");
  el.textContent = text;
  el.dataset.tone = tone;
}

function readHomeSnapshot() {
  const snapshot = safeJsonParse(HOME_CACHE_KEY, null);
  if (!snapshot || typeof snapshot !== "object" || !snapshot.data) return null;
  const savedAt = Number(snapshot.saved_at || 0);
  if (!Number.isFinite(savedAt) || savedAt <= 0) return null;
  if (Date.now() - savedAt > HOME_CACHE_MAX_AGE_MS) return null;
  return { data: snapshot.data, savedAt };
}

function saveHomeSnapshot(data) {
  try {
    localStorage.setItem(HOME_CACHE_KEY, JSON.stringify({ saved_at: Date.now(), data }));
  } catch (_) {
    // If storage is tight, keeping chat and settings matters more than a warm home snapshot.
  }
}

function readRightNowSnapshot() {
  const snapshot = safeJsonParse(RIGHT_NOW_CACHE_KEY, null);
  if (!snapshot || typeof snapshot !== "object" || !snapshot.data) return null;
  const savedAt = Number(snapshot.saved_at || 0);
  if (!Number.isFinite(savedAt) || savedAt <= 0) return null;
  if (Date.now() - savedAt > HOME_CACHE_MAX_AGE_MS) return null;
  return { data: snapshot.data, savedAt };
}

function saveRightNowSnapshot(data) {
  try {
    localStorage.setItem(RIGHT_NOW_CACHE_KEY, JSON.stringify({ saved_at: Date.now(), data }));
  } catch (_) {
    // This is a small convenience cache; live lesson data remains fetch-first.
  }
}

function readAgendaSnapshot(days) {
  const snapshot = safeJsonParse(AGENDA_CACHE_KEY, null);
  const requestedDays = Number(days || 7);
  if (snapshot && typeof snapshot === "object" && snapshot.data) {
    const savedAt = Number(snapshot.saved_at || 0);
    const snapshotDays = Number(snapshot.days || 0);
    if (Number.isFinite(savedAt) && savedAt > 0 && snapshotDays === requestedDays && Date.now() - savedAt <= HOME_CACHE_MAX_AGE_MS) {
      return { data: snapshot.data, savedAt };
    }
  }
  const homeSnapshot = readHomeSnapshot();
  const structured = homeSnapshot?.data?.agenda_structured;
  if (structured && Number(requestedDays) === Number(state.homeDays || 7)) {
    return { data: { structured, text: "" }, savedAt: homeSnapshot.savedAt };
  }
  return null;
}

function saveAgendaSnapshot(days, data) {
  try {
    localStorage.setItem(AGENDA_CACHE_KEY, JSON.stringify({ saved_at: Date.now(), days: Number(days || 7), data }));
  } catch (_) {
    // Agenda cache is a convenience layer; live fetch remains the source of truth.
  }
}

function homeSnapshotAgeLabel(savedAt) {
  const ageMs = Math.max(0, Date.now() - Number(savedAt || 0));
  const minutes = Math.round(ageMs / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  return `${hours}h ago`;
}

function plainNotificationText(text) {
  return (text || "")
    .replace(/[*_`]/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function notificationPreviewText(text) {
  const clean = plainNotificationText(text || "");
  if (clean.length <= 420) return clean;
  return `${clean.slice(0, 380).trim()}...`;
}

function saveNotifications() {
  localStorage.setItem("hira_pwa_notifications", JSON.stringify(state.notifications.slice(0, 30)));
}

function saveDismissedNotificationIds() {
  state.dismissedNotificationIds = [...new Set(state.dismissedNotificationIds.map(String).filter(Boolean))].slice(-120);
  localStorage.setItem("hira_pwa_dismissed_notification_ids", JSON.stringify(state.dismissedNotificationIds));
}

function isNotificationDismissed(id) {
  return state.dismissedNotificationIds.map(String).includes(String(id || ""));
}

function hapticTap(duration = 8) {
  if (!("vibrate" in navigator)) return;
  try {
    navigator.vibrate(duration);
  } catch (_) {
    // Some browsers expose vibrate but block it; visual feedback still covers it.
  }
}

function urlBase64ToUint8Array(value) {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const base64 = (value + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = atob(base64);
  return Uint8Array.from([...raw].map((char) => char.charCodeAt(0)));
}

function bytesEqual(left, right) {
  if (!left || !right || left.byteLength !== right.byteLength) return false;
  const a = new Uint8Array(left);
  const b = new Uint8Array(right);
  return a.every((value, index) => value === b[index]);
}

function renderNotifications() {
  const badge = $("#notificationBadge");
  const list = $("#notificationsList");
  const panel = $("#notificationsPanel");
  const count = state.notifications.length;
  badge.hidden = count === 0;
  badge.textContent = String(Math.min(count, 99));
  if (!count) {
    list.innerHTML = `
      <div class="notification-empty">
        <p>No app notifications yet.</p>
        <small>Use the toggle above to enable device alerts for nudges and check-ins.</small>
      </div>
    `;
    return;
  }
  list.innerHTML = state.notifications
    .slice(0, 12)
    .map((item) => {
      const created = item.created ? new Date(item.created).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }) : "";
      const id = escapeHtml(String(item.id || ""));
      const kind = notificationKindClass(item.kind);
      const preview = notificationPreviewText(item.body || "");
      return `
        <article class="notification-item ${kind}" data-notification-id="${id}">
          <div class="notification-item-head">
            <strong>${markdownish(item.title || "H.I.R.A")}</strong>
            ${created ? `<small>${markdownish(created)}</small>` : ""}
          </div>
          <p>${markdownish(preview)}</p>
          <div class="notification-actions">
            <button type="button" class="ghost-btn notification-done" data-notification-action="done" data-notification-id="${id}">
              <span data-lucide="check-circle" aria-hidden="true"></span>
              Done
            </button>
            <button type="button" class="ghost-btn notification-snooze" data-notification-action="snooze" data-notification-id="${id}">
              <span data-lucide="alarm-clock" aria-hidden="true"></span>
              Snooze
            </button>
            <button type="button" class="ghost-btn notification-open" data-notification-open="${id}">
              <span data-lucide="book-open" aria-hidden="true"></span>
              Read full
            </button>
            <button type="button" class="ghost-btn notification-dismiss" data-notification-dismiss="${id}">
              <span data-lucide="x" aria-hidden="true"></span>
              Dismiss
            </button>
          </div>
          <div class="notification-feedback">
            <button type="button" class="ghost-btn ${state.feedback[id] === "useful" ? "is-selected" : ""}" data-feedback-rating="useful" data-feedback-target="${id}">Useful</button>
            <button type="button" class="ghost-btn ${state.feedback[id] === "not_now" ? "is-selected" : ""}" data-feedback-rating="not_now" data-feedback-target="${id}">Not now</button>
          </div>
        </article>
      `;
    })
    .join("");
  refreshIcons(list);
}

function notificationToggleMarkup(label, tone) {
  const safeLabel = escapeHtml(label);
  return `
    <span class="notification-toggle-track" aria-hidden="true">
      <span class="notification-toggle-knob"></span>
    </span>
    <span class="notification-toggle-label">${safeLabel}</span>
  `;
}

function setNotificationButtons({ label, title, stateText, tone = "neutral", disabled = false }) {
  const panelButton = $("#enableNotificationsBtn");
  const settingsButton = $("#settingsEnableNotificationsBtn");
  const stateLabel = $("#notificationStateText");
  const panelStateLabel = $("#notificationPanelStateText");
  const notificationButton = $("#notificationsBtn");
  [panelButton, settingsButton].forEach((button) => {
    if (!button) return;
    button.innerHTML = notificationToggleMarkup(label, tone);
    button.title = title;
    button.disabled = disabled;
    button.setAttribute("aria-pressed", tone === "ok" ? "true" : "false");
    button.dataset.notificationState = tone;
    button.classList.toggle("notification-enabled", tone === "ok");
    button.classList.toggle("notification-warning", tone === "warn");
  });
  if (notificationButton) {
    notificationButton.classList.toggle("notification-enabled", tone === "ok");
    notificationButton.classList.toggle("notification-warning", tone === "warn");
    notificationButton.title = stateText || title;
  }
  if (stateLabel) {
    stateLabel.textContent = stateText || title;
    stateLabel.classList.toggle("status-ok", tone === "ok");
    stateLabel.classList.toggle("status-warn", tone === "warn");
  }
  if (panelStateLabel) {
    panelStateLabel.textContent = stateText || title;
    panelStateLabel.classList.toggle("status-ok", tone === "ok");
    panelStateLabel.classList.toggle("status-warn", tone === "warn");
  }
}

async function updateNotificationControls() {
  if (!("Notification" in window)) {
    setNotificationButtons({
      label: "Unavailable",
      title: "This browser does not support app notifications.",
      stateText: "State: unavailable in this browser.",
      tone: "warn",
      disabled: true,
    });
    return;
  }

  if (Notification.permission === "denied") {
    setNotificationButtons({
      label: "Blocked",
      title: "Notifications are blocked in browser settings.",
      stateText: "State: blocked in browser settings.",
      tone: "warn",
      disabled: true,
    });
    return;
  }

  if (Notification.permission !== "granted") {
    setNotificationButtons({
      label: "Off",
      title: "Ask this browser for notification permission.",
      stateText: "State: not enabled on this device.",
      disabled: false,
    });
    return;
  }

  if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
    setNotificationButtons({
      label: "On",
      title: "Browser notifications are enabled on this device.",
      stateText: "State: enabled on this device.",
      tone: "ok",
      disabled: true,
    });
    return;
  }

  setNotificationButtons({
    label: "On",
    title: "Browser notification permission is enabled.",
    stateText: "State: enabled; checking push connection.",
    tone: "ok",
    disabled: true,
  });

  try {
    const registration = await navigator.serviceWorker.ready;
    let subscription = await registration.pushManager.getSubscription();
    if (state.sessionUnlocked) {
      const synced = await ensurePushSubscription().catch((error) => {
        setStatus(`Push reconnect: ${error.message}`, "warn");
        return false;
      });
      if (synced) subscription = await registration.pushManager.getSubscription();
    }
    setNotificationButtons({
      label: subscription ? "On" : "Setup",
      title: subscription
        ? "Browser permission and push subscription are active. H.I.R.A will keep this device synced."
        : "Browser permission is on, but push still needs to be connected.",
      stateText: subscription
        ? "State: enabled and connected on this device."
        : "State: browser permission is on, push is not connected yet.",
      tone: subscription ? "ok" : "warn",
      disabled: false,
    });
  } catch (_) {
    setNotificationButtons({
      label: "On",
      title: "Browser notification permission is enabled.",
      stateText: "State: enabled on this device.",
      tone: "ok",
      disabled: true,
    });
  }
}

async function enableNotifications() {
  if (!("Notification" in window)) {
    setStatus("This browser does not support app notifications.", "warn");
    await updateNotificationControls();
    return false;
  }
  if (Notification.permission === "granted") {
    const subscribed = await subscribeForPushNotifications().catch((error) => {
      setStatus(`Notification setup: ${error.message}`, "warn");
      return false;
    });
    setStatus(
      subscribed ? "App notifications are enabled." : "Browser notifications are enabled, but push is not connected.",
      subscribed ? "ok" : "warn"
    );
    await updateNotificationControls();
    return subscribed;
  }
  if (Notification.permission === "denied") {
    setStatus("Notifications are blocked in browser settings.", "warn");
    await updateNotificationControls();
    return false;
  }
  const permission = await Notification.requestPermission();
  if (permission !== "granted") {
    setStatus("Notifications were not enabled.", "warn");
    await updateNotificationControls();
    return false;
  }
  const subscribed = await subscribeForPushNotifications().catch((error) => {
    setStatus(`Notification setup: ${error.message}`, "warn");
    return false;
  });
  setStatus(
    subscribed ? "App notifications are enabled." : "Browser notifications are enabled, but push is not connected.",
    subscribed ? "ok" : "warn"
  );
  await updateNotificationControls();
  return subscribed;
}

async function subscribeForPushNotifications() {
  return ensurePushSubscription({ force: true });
}

async function ensurePushSubscription({ force = false } = {}) {
  if (!("serviceWorker" in navigator) || !("PushManager" in window)) return false;
  if (!state.sessionUnlocked) return false;
  const config = await api("/api/notifications/config", { headers: headers(false) });
  const publicKey = (config.vapid_public_key || "").trim();
  if (!publicKey) return false;
  const registration = await navigator.serviceWorker.ready;
  const applicationServerKey = urlBase64ToUint8Array(publicKey);
  const existing = await registration.pushManager.getSubscription();
  const existingKey = existing?.options?.applicationServerKey || null;
  const shouldReplace = existing && existingKey && !bytesEqual(existingKey, applicationServerKey);
  if (shouldReplace) {
    await existing.unsubscribe();
  }
  const subscription = shouldReplace || !existing ? await registration.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey,
  }) : existing;
  await syncPushSubscription(subscription, { force });
  return true;
}

async function syncPushSubscription(subscription, { force = false } = {}) {
  if (!subscription) return false;
  if (!state.sessionUnlocked) return false;
  const now = Date.now();
  const displayMode = isStandalonePwa() ? "standalone" : "browser";
  const endpoint = String(subscription.endpoint || "");
  const recentlySynced = now - state.lastPushSyncAt < 1000 * 60 * 30;
  const modeChanged = displayMode !== state.lastPushSyncMode;
  const endpointChanged = endpoint && endpoint !== state.lastPushSyncEndpoint;
  if (!force && recentlySynced && !modeChanged && !endpointChanged) return true;
  await api("/api/notifications/subscribe", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({
      subscription,
      display_mode: displayMode,
      app_version: APP_VERSION,
      user_agent: navigator.userAgent || "",
    }),
  });
  state.lastPushSyncAt = now;
  state.lastPushSyncMode = displayMode;
  state.lastPushSyncEndpoint = endpoint;
  localStorage.setItem("hira_pwa_last_push_sync_at", String(now));
  localStorage.setItem(PUSH_SYNC_MODE_KEY, displayMode);
  if (endpoint) localStorage.setItem(PUSH_SYNC_ENDPOINT_KEY, endpoint);
  return true;
}

async function showSystemNotification(item) {
  if (!("Notification" in window) || Notification.permission !== "granted") return;
  const title = item.title || "H.I.R.A";
  const body = plainNotificationText(item.body || "").slice(0, 240);
  const options = {
    body,
    tag: `hira-${item.id}`,
    icon: "/static/icon.svg",
    badge: "/static/icon.svg",
    data: { id: item.id },
  };
  try {
    const registration = await navigator.serviceWorker?.ready;
    if (registration?.showNotification) {
      await registration.showNotification(title, options);
      return;
    }
  } catch (_) {
    // Fall through to the page Notification constructor.
  }
  new Notification(title, options);
}

function chatPromptNotification(item) {
  const source = String(item?.source || "");
  if (!/^(checkin|nudge):/.test(source)) return "";
  const body = plainNotificationText(item.body || "");
  if (!body) return "";
  if (source.startsWith("checkin:")) {
    return `${body}\n\nReply yes, done, or alhamdulillah here and I’ll stop asking for today.`;
  }
  return body;
}

function mirrorNotificationToChat(item) {
  const id = String(item?.id || "");
  if (!id || state.chatNotificationIds.map(String).includes(id)) return false;
  const text = chatPromptNotification(item);
  if (!text) return false;
  addMessage("hira", text);
  state.chatNotificationIds.push(id);
  saveChatNotificationIds();
  return true;
}

function mirrorStoredNotificationsToChat() {
  let mirrored = 0;
  for (const item of state.notifications) {
    if (mirrorNotificationToChat(item)) mirrored += 1;
    if (mirrored >= 3) break;
  }
}

function isStandalonePwa() {
  return Boolean(
    window.matchMedia?.("(display-mode: standalone)")?.matches ||
    window.matchMedia?.("(display-mode: fullscreen)")?.matches ||
    window.navigator.standalone
  );
}

function reportClientModeToServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  navigator.serviceWorker.controller?.postMessage({
    type: "HIRA_CLIENT_MODE",
    standalone: isStandalonePwa(),
  });
}

function rememberNotification(item) {
  if (isNotificationDismissed(item?.id)) return false;
  const existingIndex = state.notifications.findIndex((existing) => String(existing.id) === String(item.id));
  if (existingIndex !== -1) {
    const existing = state.notifications[existingIndex];
    const incomingBody = String(item.body || "");
    const existingBody = String(existing.body || "");
    const useIncomingBody = incomingBody.length > existingBody.length && !incomingBody.includes("Open H.I.R.A");
    state.notifications[existingIndex] = {
      ...existing,
      ...item,
      body: useIncomingBody ? incomingBody : existingBody,
      created: existing.created || item.created,
    };
    saveNotifications();
    renderNotifications();
    mirrorNotificationToChat(state.notifications[existingIndex]);
    return false;
  }
  state.notifications.unshift(item);
  state.notifications = state.notifications.slice(0, 30);
  saveNotifications();
  renderNotifications();
  mirrorNotificationToChat(item);
  return true;
}

function rememberPushedNotification(item) {
  const notification = {
    id: String(item?.id || ""),
    kind: String(item?.kind || "reminder"),
    title: String(item?.title || "H.I.R.A"),
    body: String(item?.body || ""),
    source: String(item?.source || ""),
    created: item?.created || new Date().toISOString(),
  };
  if (!notification.id || !notification.body) return false;
  return rememberNotification(notification);
}

function notificationMetaText(item) {
  const parts = [];
  if (item?.created) {
    try {
      parts.push(new Date(item.created).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }));
    } catch (_) {
      parts.push(String(item.created));
    }
  }
  if (item?.kind) parts.push(String(item.kind).replace(/_/g, " "));
  return parts.join(" · ");
}

function renderNotificationReader(item, loading = false) {
  const reader = $("#notificationReader");
  const title = $("#notificationReaderTitle");
  const meta = $("#notificationReaderMeta");
  const body = $("#notificationReaderBody");
  const dismiss = $("#notificationReaderDismissBtn");
  const done = $("#notificationReaderDoneBtn");
  const snooze = $("#notificationReaderSnoozeBtn");
  const cleanTitle = item?.title || "H.I.R.A";
  title.innerHTML = markdownish(cleanTitle);
  meta.textContent = loading ? "Loading full briefing..." : notificationMetaText(item);
  body.innerHTML = loading ? "<div>Loading full briefing...</div>" : renderTextBlock(item?.body || "No briefing text was saved for this notification.");
  dismiss.dataset.notificationDismiss = item?.id || "";
  done.dataset.notificationAction = "done";
  done.dataset.notificationId = item?.id || "";
  snooze.dataset.notificationAction = "snooze";
  snooze.dataset.notificationId = item?.id || "";
  reader.hidden = false;
  document.body.classList.add("notification-reader-open");
  refreshIcons(reader);
}

function notificationFromPayload(item = {}) {
  return {
    id: String(item?.id || ""),
    kind: String(item?.kind || "notice"),
    title: String(item?.title || "H.I.R.A"),
    body: String(item?.body || ""),
    source: String(item?.source || ""),
    created: item?.created || new Date().toISOString(),
  };
}

function closeNotificationReader() {
  $("#notificationReader").hidden = true;
  state.activeNotificationId = "";
  state.activeNotificationItem = null;
  document.body.classList.remove("notification-reader-open");
}

async function fetchNotificationDetail(id) {
  const data = await api(`/api/notifications/${encodeURIComponent(id)}`, { headers: headers(false) });
  return data.notification || null;
}

async function openNotificationReader(item = {}) {
  const notification = notificationFromPayload(item);
  if (!notification.id && !notification.body) return;
  state.activeNotificationId = notification.id;
  state.activeNotificationItem = notification;
  if (notification.body) rememberPushedNotification(notification);
  renderNotificationReader(notification, Boolean(notification.id && !notification.body));
  let fullNotification = notification;
  if (notification.id) {
    try {
      const fetched = await fetchNotificationDetail(notification.id);
      if (fetched && state.activeNotificationId === notification.id) {
        fullNotification = fetched;
        state.activeNotificationItem = fetched;
        rememberNotification(fetched);
        renderNotificationReader(fetched, false);
        await markNotificationsSeen([notification.id]);
      }
    } catch (error) {
      renderNotificationReader(notification, false);
      setStatus(`Could not load the full notification: ${error.message}`, "warn");
      return;
    }
  }
  const title = plainNotificationText(fullNotification.title || "Notification");
  setStatus(title ? `Opened ${title}.` : "Opened notification.", "ok");
}

function rememberNotificationFromUrl() {
  const params = new URLSearchParams(window.location.search);
  if (!params.has("notification_id")) return;
  const item = notificationFromPayload({
    id: params.get("notification_id"),
    kind: params.get("notification_kind") || "reminder",
    source: params.get("notification_source") || "",
    title: params.get("notification_title") || "H.I.R.A",
    body: params.get("notification_body") || "",
  });
  const action = params.get("notification_action") || "";
  params.delete("notification_id");
  params.delete("notification_kind");
  params.delete("notification_source");
  params.delete("notification_title");
  params.delete("notification_body");
  params.delete("notification_action");
  const query = params.toString();
  const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
  window.history.replaceState({}, "", nextUrl);
  if (action) {
    performNotificationAction(action, item);
  } else {
    openNotificationReader(item);
  }
}

async function markNotificationsSeen(ids) {
  if (!ids.length) return;
  try {
    await api("/api/notifications/seen", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ ids }),
    });
  } catch (error) {
    setStatus(`Notification sync: ${error.message}`, "warn");
  }
}

async function dismissNotification(id) {
  const notificationId = String(id || "");
  if (!notificationId) return;
  state.dismissedNotificationIds.push(notificationId);
  saveDismissedNotificationIds();
  state.notifications = state.notifications.filter((item) => String(item.id) !== notificationId);
  saveNotifications();
  renderNotifications();
  try {
    await api("/api/notifications/archive", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ ids: [notificationId] }),
    });
    setStatus("Notification dismissed.", "ok");
  } catch (error) {
    setStatus(`Notification hidden here; server dismiss failed: ${error.message}`, "warn");
  }
}

async function sendInsightFeedback(target, rating, kind = "notification") {
  const key = String(target || "");
  try {
    await api("/api/insights/feedback", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ kind, target: key, rating }),
    });
    if (key) {
      state.feedback[key] = rating;
      localStorage.setItem("hira_pwa_feedback", JSON.stringify(state.feedback));
      renderNotifications();
    }
    setStatus(rating === "useful" ? "Noted. More like this." : "Noted. I will quieten signals like that.", "ok");
  } catch (error) {
    setStatus(`Could not save feedback: ${error.message}`, "warn");
  }
}

async function performNotificationAction(action, item = {}) {
  const notification = {
    id: String(item?.id || ""),
    kind: String(item?.kind || "reminder"),
    title: String(item?.title || "H.I.R.A"),
    body: String(item?.body || ""),
    source: String(item?.source || ""),
  };
  if (!notification.id || !action) return;
  rememberPushedNotification(notification);
  try {
    const data = await api("/api/notifications/action", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({
        id: notification.id,
        action,
        snooze_minutes: action === "snooze" ? 30 : 0,
      }),
    });
    if (["done", "snooze", "not_useful", "not_now"].includes(action)) {
      state.dismissedNotificationIds.push(notification.id);
      saveDismissedNotificationIds();
      state.notifications = state.notifications.filter((existing) => String(existing.id) !== notification.id);
    }
    if (["useful", "not_useful", "not_now"].includes(action)) {
      state.feedback[notification.id] = action;
      localStorage.setItem("hira_pwa_feedback", JSON.stringify(state.feedback));
    }
    renderNotifications();
    const labels = {
      done: data.nudge_id
        ? "Nudge cleared."
        : data.prayer_key
          ? "Prayer cleared for today."
        : data.completed === false
          ? "Noted, but I could not find the linked item."
          : "Marked done.",
      snooze: "Snoozed for 30 minutes.",
      useful: "Noted. More like this.",
      not_useful: "Noted. I will quieten signals like that.",
      not_now: "Noted. I will quieten signals like that.",
    };
    setStatus(labels[action] || "Notification action saved.", data.completed === false ? "warn" : "ok");
  } catch (error) {
    setStatus(`Notification action failed: ${error.message}`, "warn");
  }
}

async function pollNotifications() {
  try {
    const data = await api("/api/notifications?limit=12", { headers: headers(false) });
    const items = data.notifications || [];
    if (!items.length) return;
    const fresh = [];
    const activeItems = items.filter((item) => !isNotificationDismissed(item.id));
    const dismissedIds = items
      .filter((item) => isNotificationDismissed(item.id))
      .map((item) => item.id);
    if (dismissedIds.length) {
      api("/api/notifications/archive", {
        method: "POST",
        headers: headers(),
        body: JSON.stringify({ ids: dismissedIds }),
      }).catch(() => {});
    }
    for (const item of activeItems) {
      if (rememberNotification(item)) {
        fresh.push(item);
      }
    }
    await markNotificationsSeen(activeItems.map((item) => item.id));
    if (fresh.length) {
      Promise.allSettled(fresh.map((item) => showSystemNotification(item))).catch(() => {});
    }
    if (fresh.length) setStatus(`${fresh.length} app notification${fresh.length === 1 ? "" : "s"} received.`, "ok");
  } catch (error) {
    if (!/token/i.test(error.message)) setStatus(`Notifications: ${error.message}`, "warn");
  }
}

async function sendTestNotification(event) {
  const button = event?.currentTarget || $("#testNotificationsBtn");
  const previousLabel = button?.textContent || "Test push";
  if (button) {
    button.disabled = true;
    button.textContent = "Testing...";
  }
  setStatus("Testing push notification...", "ok");
  try {
    if ("Notification" in window && Notification.permission === "granted") {
      await ensurePushSubscription({ force: true });
    }
    const data = await api("/api/notifications/test", {
      method: "POST",
      headers: headers(false),
    });
    rememberNotification(data.notification);
    setStatus(data.sent ? "Test push sent to this device." : "Test notification queued; push may need reconnecting.", data.sent ? "ok" : "warn");
  } catch (error) {
    setStatus(`Notification test failed: ${error.message}`, "error");
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = previousLabel;
    }
  }
}

function renderHealth(data) {
  const el = $("#healthOutput");
  if (!el) return;
  const prayerRows = (data.prayers?.prayers || [])
    .map((item) => `${item.label} ${item.time}${item.prompted ? ` sent ${item.prompted_at}` : ""}`)
    .join(" · ");
  const deliveryRows = (data.recent_delivery_log || [])
    .slice()
    .reverse()
    .map((item) => {
      const errors = Object.entries(item.errors || {})
        .map(([key, value]) => `${key} ${value}`)
        .join(", ");
      const suffix = [
        item.expired ? `expired ${item.expired}` : "",
        item.payload_bytes ? `${item.payload_bytes} bytes` : "",
        errors ? `errors: ${errors}` : "",
        item.last_error ? `last: ${item.last_error}` : "",
      ].filter(Boolean).join("; ");
      return `${item.source || item.kind || "push"} ${item.sent}/${item.attempted}${suffix ? ` (${suffix})` : ""}`;
    })
    .join(" · ");
  const outcomeRows = Object.entries(data.outcome_actions || {})
    .map(([key, value]) => `${key} ${value}`)
    .join(" · ");
  const recovery = data.push_recovery || {};
  const briefing = data.briefing_delivery || {};
  const recoveryText = [
    recovery.last_attempt_at ? `last attempt ${recovery.last_attempt_source || "push"} ${recovery.last_attempt_sent || 0}/${recovery.last_attempted || 0}` : "",
    recovery.last_success_at ? `last success ${recovery.last_success_source || "push"} ${recovery.last_success_at}` : "",
    recovery.recent_failure_count ? `${recovery.recent_failure_count} recent misses` : "",
    recovery.issue ? `issue: ${recovery.issue}` : "",
  ].filter(Boolean).join(" · ");
  el.hidden = false;
  el.innerHTML = `
    <div class="status-row"><span>Push keys</span><strong>${data.push_public_key && data.push_private_key ? "Ready" : "Missing"}</strong></div>
    <div class="status-row"><span>Subscriptions</span><strong>${data.subscription_count || 0}</strong></div>
    <div class="status-row"><span>Standalone subs</span><strong>${data.standalone_subscription_count || 0}</strong></div>
    <div class="status-row"><span>This device</span><strong>${data.current_client_subscribed ? "Connected" : "Not connected"}</strong></div>
    <div class="status-row"><span>This mode</span><strong>${data.current_client_display_mode || appDisplayMode()}</strong></div>
    <div class="status-row"><span>Stale subs</span><strong>${data.stale_subscription_count || 0}</strong></div>
    <div class="status-row"><span>Queued</span><strong>${data.queued_notification_count || 0}</strong></div>
    <div class="status-row"><span>Recovery</span><strong>${data.push_recovery_enabled ? "On" : "Off"}</strong></div>
    <div class="status-row"><span>Delivery</span><strong>${recovery.status || "unknown"}</strong></div>
    <div class="status-row"><span>Digest</span><strong>${briefing.overall || "unknown"}</strong></div>
    <p class="subtle">${markdownish(recoveryText || "No recovery data yet.")}</p>
    <p class="subtle">${markdownish(deliveryRows || "No recent push delivery attempts logged.")}</p>
    <p class="subtle">${markdownish(outcomeRows || "No notification feedback captured yet.")}</p>
    <p class="subtle">${markdownish(prayerRows || "Prayer status unavailable.")}</p>
  `;
}

const sgdFormatter = new Intl.NumberFormat("en-SG", {
  style: "decimal",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatSgd(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount)) return "S$0.00";
  if (amount > 0 && amount < 0.01) return "<S$0.01";
  return `S$${sgdFormatter.format(amount)}`;
}

function formatTokenCount(value) {
  const amount = Number(value || 0);
  if (!Number.isFinite(amount) || amount <= 0) return "0";
  if (amount >= 1_000_000) return `${(amount / 1_000_000).toFixed(amount >= 10_000_000 ? 1 : 2)}M`;
  if (amount >= 1_000) return `${(amount / 1_000).toFixed(amount >= 10_000 ? 0 : 1)}k`;
  return String(Math.round(amount));
}

function apiSpendRow(label, value, tone = "") {
  const cleanTone = tone ? ` class="${tone}"` : "";
  return `<div class="status-row"><span>${escapeHtml(label)}</span><strong${cleanTone}>${escapeHtml(String(value || "--"))}</strong></div>`;
}

function apiSpendBucket(label, bucket = {}) {
  const requests = Number(bucket.requests || 0);
  const cacheRatio = Math.round(Number(bucket.cache_hit_ratio || 0) * 100);
  return `
    <div class="api-spend-bucket">
      <div>
        <span>${escapeHtml(label)}</span>
        <strong>${formatSgd(bucket.estimated_sgd)}</strong>
      </div>
      <small>${requests} request${requests === 1 ? "" : "s"} · ${formatTokenCount(bucket.total_tokens)} tokens · ${cacheRatio}% cache</small>
    </div>
  `;
}

function apiSpendBreakdown(title, items = {}) {
  const rows = Object.entries(items || {});
  if (!rows.length) {
    return `
      <section class="api-spend-breakdown">
        <h4>${escapeHtml(title)}</h4>
        <p class="subtle">No tracked calls yet.</p>
      </section>
    `;
  }
  return `
    <section class="api-spend-breakdown">
      <h4>${escapeHtml(title)}</h4>
      <div class="api-spend-buckets">
        ${rows.map(([label, bucket]) => apiSpendBucket(label, bucket)).join("")}
      </div>
    </section>
  `;
}

function renderApiSpend(data) {
  const el = $("#apiSpendOutput");
  if (!el) return;
  const usage = data?.runtime?.api_usage || data?.api_usage || data?.runtime?.openai_usage || data?.openai_usage || {};
  const today = usage.today || {};
  const week = usage.last_7d || {};
  const lastRequest = usage.last_request || {};
  const providerLabel = usage.provider ? String(usage.provider).replace(/^./, (char) => char.toUpperCase()) : "OpenAI";
  const nativeTools = Object.entries(today.native_tools || {})
    .filter(([, count]) => Number(count || 0) > 0)
    .map(([tool, count]) => `${tool.replaceAll("_", " ")} ${count}`)
    .join(" · ");
  const lastSeen = lastRequest.at ? new Date(lastRequest.at).toLocaleString("en-SG", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }) : "No call yet";
  el.hidden = false;
  el.innerHTML = `
    <div class="api-spend-summary">
      <div class="api-spend-total">
        <span>Today</span>
        <strong>${formatSgd(today.estimated_sgd)}</strong>
        <small>${today.requests || 0} request${Number(today.requests || 0) === 1 ? "" : "s"}</small>
      </div>
      <div class="api-spend-total">
        <span>Last 7 days</span>
        <strong>${formatSgd(week.estimated_sgd)}</strong>
        <small>${formatTokenCount(week.total_tokens)} tokens</small>
      </div>
    </div>
    <div class="api-spend-rows">
      ${apiSpendRow("Provider", providerLabel)}
      ${apiSpendRow("Tracking", usage.tracking || "unknown", usage.tracking === "enabled" ? "status-ok" : "status-warn")}
      ${apiSpendRow("Cache hit", `${Math.round(Number(today.cache_hit_ratio || 0) * 100)}%`)}
      ${apiSpendRow("Input", `${formatTokenCount(today.input_tokens)} tokens`)}
      ${apiSpendRow("Output", `${formatTokenCount(today.output_tokens)} tokens`)}
      ${apiSpendRow("Reasoning", `${formatTokenCount(today.reasoning_tokens)} tokens`)}
      ${apiSpendRow("Native tools", nativeTools || "none")}
      ${apiSpendRow("FX", `1 USD = S$${Number(usage.sgd_per_usd || 0).toFixed(2)}`)}
      ${apiSpendRow("Last call", lastSeen)}
    </div>
    ${apiSpendBreakdown("By model today", usage.models_today)}
    ${apiSpendBreakdown("By route today", usage.tiers_today)}
    <p class="subtle">${escapeHtml(usage.note || "Estimated from tracked OpenAI calls. Billing dashboard remains the source of truth.")}</p>
  `;
}

async function loadApiSpend({ quiet = false } = {}) {
  const button = $("#checkApiSpendBtn");
  const output = $("#apiSpendOutput");
  const previousLabel = button?.textContent || "Refresh";
  try {
    if (button) {
      button.disabled = true;
      button.textContent = "Checking";
    }
    if (output && !quiet) {
      output.innerHTML = "<div class=\"empty-state compact\">Checking the bill diary...</div>";
    }
    const data = await api("/api/admin/status", { headers: headers(false) });
    renderApiSpend(data);
    if (!quiet) setStatus("API spend refreshed.", "ok");
  } catch (error) {
    if (output) {
      output.innerHTML = `<div class="empty-state compact">API spend unavailable: ${escapeHtml(error.message)}</div>`;
    }
    if (!quiet) setStatus(`API spend check failed: ${error.message}`, "error");
  } finally {
    if (button) {
      button.disabled = false;
      button.innerHTML = `<span data-lucide="receipt-text" aria-hidden="true"></span>Refresh`;
      refreshIcons(button);
    } else if (previousLabel) {
      refreshIcons();
    }
  }
}

function briefingDeliveryTone(status) {
  const clean = String(status || "").toLowerCase();
  if (clean === "delivered" || clean === "pending") return "ok";
  if (clean === "queued" || clean === "recovering") return "warn";
  if (clean === "missed" || clean === "unconfirmed") return "danger";
  return "off";
}

function renderBriefingDelivery(delivery = {}) {
  const rowsEl = $("#briefingDeliveryRows");
  const summaryEl = $("#briefingDeliverySummary");
  const badgeEl = $("#briefingDeliveryBadge");
  if (!rowsEl || !summaryEl || !badgeEl) return;
  const slots = Array.isArray(delivery.slots) ? delivery.slots : [];
  const overall = String(delivery.overall || "unknown").toLowerCase();
  const badgeTone = overall === "attention" ? "danger" : overall === "watching" ? "warn" : overall === "ok" ? "ok" : "off";
  summaryEl.textContent = delivery.summary || "Digest delivery status unavailable.";
  badgeEl.textContent = overall === "attention" ? "CHECK" : overall === "watching" ? "WATCH" : overall === "ok" ? "OK" : "WAIT";
  badgeEl.className = `briefing-delivery-badge status-${badgeTone}`;
  if (!slots.length) {
    rowsEl.innerHTML = `
      <div class="briefing-delivery-row is-empty">
        <span>Delivery</span>
        <strong>Unavailable</strong>
        <small>No digest delivery data returned.</small>
      </div>
    `;
    return;
  }
  rowsEl.innerHTML = slots.map((slot) => {
    const status = String(slot.status || "unknown");
    const tone = briefingDeliveryTone(status);
    const label = escapeHtml(slot.label || slot.slot || "Digest");
    const time = escapeHtml(slot.time || "--:--");
    const detail = escapeHtml(slot.detail || "No detail yet.");
    return `
      <div class="briefing-delivery-row status-${tone}">
        <span>${label} <small>${time}</small></span>
        <strong>${escapeHtml(status.toUpperCase())}</strong>
        <small>${detail}</small>
      </div>
    `;
  }).join("");
}

function versionRow(label, value, tone = "") {
  const cleanTone = tone ? ` class="${tone}"` : "";
  return `<div class="status-row"><span>${label}</span><strong${cleanTone}>${markdownish(String(value || "--"))}</strong></div>`;
}

function appDisplayMode() {
  if (isStandalonePwa()) return "Standalone";
  return "Browser tab";
}

function serviceWorkerVersionRequest(worker, timeoutMs = 900) {
  if (!worker) return Promise.resolve(null);
  return new Promise((resolve) => {
    const channel = new MessageChannel();
    const timer = window.setTimeout(() => resolve(null), timeoutMs);
    channel.port1.onmessage = (event) => {
      window.clearTimeout(timer);
      resolve(event.data || null);
    };
    try {
      worker.postMessage({ type: "GET_HIRA_VERSION" }, [channel.port2]);
    } catch (_) {
      window.clearTimeout(timer);
      resolve(null);
    }
  });
}

async function readAppVersionState() {
  const stateInfo = {
    appVersion: APP_VERSION,
    appScript: APP_SCRIPT,
    expectedCache: EXPECTED_SW_CACHE,
    displayMode: appDisplayMode(),
    controller: navigator.serviceWorker?.controller ? "Controlled" : "No controller",
    updateState: "Unknown",
    scope: "--",
    swAppVersion: "--",
    swCache: "--",
    serverCommit: "--",
    serverTime: "--",
  };
  if ("serviceWorker" in navigator) {
    try {
      const registration = await navigator.serviceWorker.ready;
      stateInfo.scope = registration.scope || "--";
      stateInfo.updateState = registration.waiting ? "Waiting" : registration.installing ? "Installing" : "Active";
      const worker = registration.active || navigator.serviceWorker.controller;
      const swInfo = await serviceWorkerVersionRequest(worker);
      if (swInfo) {
        stateInfo.swAppVersion = swInfo.appVersion || "--";
        stateInfo.swCache = swInfo.cacheName || "--";
      }
    } catch (_) {
      stateInfo.updateState = "Unavailable";
    }
  } else {
    stateInfo.updateState = "No service worker";
  }
  try {
    const server = await api("/api/app/version", { headers: headers(false) });
    stateInfo.serverCommit = server.git_commit || "--";
    stateInfo.serverTime = server.server_time || "--";
  } catch (_) {
    stateInfo.serverCommit = "Unavailable";
  }
  return stateInfo;
}

async function renderAppVersion() {
  const el = $("#versionOutput");
  if (!el) return;
  el.innerHTML = versionRow("App", `${APP_VERSION} · ${APP_SCRIPT}`);
  const info = await readAppVersionState();
  const cacheOk = info.swCache === EXPECTED_SW_CACHE;
  const appOk = info.swAppVersion === APP_VERSION;
  const updateTone = cacheOk && appOk && info.controller === "Controlled" ? "status-ok" : "status-warn";
  el.innerHTML = [
    versionRow("App", `${info.appVersion} · ${info.appScript}`),
    versionRow("Service worker", `${info.swAppVersion} · ${info.swCache}`, cacheOk && appOk ? "status-ok" : "status-warn"),
    versionRow("Controller", `${info.controller} · ${info.updateState}`, updateTone),
    versionRow("Mode", info.displayMode, info.displayMode === "Standalone" ? "status-ok" : "status-warn"),
    versionRow("Server", info.serverCommit),
    versionRow("Checked", info.serverTime || new Date().toLocaleTimeString("en-SG", { hour: "2-digit", minute: "2-digit", hour12: false })),
  ].join("");
}

function ledgerStatusClass(status) {
  const clean = String(status || "").toLowerCase();
  if (["saved", "done", "snoozed"].includes(clean)) return "status-ok";
  if (["blocked", "failed"].includes(clean)) return "status-warn";
  return "status-off";
}

function ledgerActionLabel(action) {
  const labels = {
    "task.done": "Task done",
    "notification.done": "Notification done",
    "notification.snooze": "Notification snoozed",
    "gmail.draft": "Gmail draft",
    "classops.assignment": "ClassOps assignment",
    "classops.content_override": "ClassOps content",
    "create_calendar_event": "Calendar event",
    "create_proactive_nudge": "Nudge",
    "add_reminder": "Reminder",
    "action_ledger.undo": "Undo",
  };
  return labels[action] || String(action || "Action").replaceAll("_", " ");
}

function renderActionLedger(entries = state.actionLedger) {
  const el = $("#actionLedgerList");
  if (!el) return;
  const items = Array.isArray(entries) ? entries : [];
  if (!items.length) {
    el.innerHTML = "<div class='empty-state compact'>No recent action receipts.</div>";
    return;
  }
  el.innerHTML = items.map((item) => {
    const reviewed = Boolean(item.reviewed);
    const undone = String(item.undo_status || "") === "undone";
    const canUndo = !undone && ["saved", "done", "snoozed"].includes(String(item.status || "").toLowerCase());
    const detail = [item.date, item.source, item.client_id].filter(Boolean).join(" · ");
    return `
      <article class="action-ledger-item ${reviewed ? "is-reviewed" : ""}">
        <div>
          <span class="action-ledger-meta ${ledgerStatusClass(item.status)}">${markdownish(item.status || "logged")}</span>
          <strong>${markdownish(ledgerActionLabel(item.action))}</strong>
          <p>${markdownish(item.subject || item.result || "No subject recorded.")}</p>
          ${detail ? `<small>${markdownish(detail)}</small>` : ""}
          ${item.undo_result ? `<small class="action-ledger-undo">${markdownish(item.undo_result)}</small>` : ""}
        </div>
        <div class="action-ledger-actions">
          <button type="button" class="ghost-btn" data-ledger-review="${escapeHtml(item.id)}">${reviewed ? "Reviewed" : "Review"}</button>
          <button type="button" class="ghost-btn" data-ledger-undo="${escapeHtml(item.id)}" ${canUndo ? "" : "disabled"}>Undo</button>
        </div>
      </article>
    `;
  }).join("");
  refreshIcons(el);
}

async function loadActionLedger({ quiet = false } = {}) {
  const el = $("#actionLedgerList");
  if (el && !quiet) el.innerHTML = "<div class='empty-state compact'>Loading receipts...</div>";
  try {
    const data = await api("/api/action-ledger?limit=12&include_reviewed=true", { headers: headers(false) });
    state.actionLedger = data.entries || [];
    renderActionLedger();
    if (!quiet) setStatus("Action ledger refreshed.", "ok");
  } catch (error) {
    if (el) el.innerHTML = `<div class="empty-state compact">Action ledger unavailable: ${markdownish(error.message)}</div>`;
    if (!quiet) setStatus(`Action ledger unavailable: ${error.message}`, "warn");
  }
}

async function reviewActionEntry(entryId) {
  try {
    const data = await api(`/api/action-ledger/${encodeURIComponent(entryId)}/review`, {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ reviewed: true }),
    });
    state.actionLedger = state.actionLedger.map((item) => String(item.id) === String(entryId) ? data.entry : item);
    renderActionLedger();
    setStatus("Action receipt reviewed.", "ok");
  } catch (error) {
    setStatus(`Could not review action: ${error.message}`, "warn");
  }
}

async function undoActionEntry(entryId) {
  try {
    const data = await api(`/api/action-ledger/${encodeURIComponent(entryId)}/undo`, {
      method: "POST",
      headers: headers(false),
    });
    state.actionLedger = state.actionLedger.map((item) => String(item.id) === String(entryId) ? data.entry : item);
    renderActionLedger();
    setStatus(data.result || "Undo checked.", data.ok ? "ok" : "warn");
    if (data.ok) {
      await loadHome({ force: true, background: true, useCache: false });
    }
  } catch (error) {
    setStatus(`Undo failed: ${error.message}`, "warn");
  }
}

async function checkNotificationHealth() {
  try {
    const data = await api("/api/notifications/health", { headers: headers(false) });
    renderHealth(data);
    setStatus("Notification health checked.", "ok");
  } catch (error) {
    setStatus(`Health check failed: ${error.message}`, "error");
  }
}

function startNotificationPolling() {
  if (state.notificationPoll) clearInterval(state.notificationPoll);
  pollNotifications();
  state.notificationPoll = setInterval(pollNotifications, 60000);
}

function countMeaningfulLines(text) {
  return (text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("*") && !line.startsWith("_")).length;
}

function renderSegments(target, filled, total = 12, tone = "accent") {
  const el = typeof target === "string" ? $(target) : target;
  if (!el) return;
  const amount = Math.max(0, Math.min(total, filled));
  el.innerHTML = Array.from({ length: total }, (_, index) => {
    const active = index < amount ? "active" : "";
    return `<span class="segment ${active} ${tone}"></span>`;
  }).join("");
}

function renderSegmentsAll(selector, filled, total = 12, tone = "accent") {
  document.querySelectorAll(selector).forEach((el) => renderSegments(el, filled, total, tone));
}

function segmentMarkup(filled, total = 12, tone = "accent") {
  const amount = Math.max(0, Math.min(total, filled));
  return Array.from({ length: total }, (_, index) => {
    const active = index < amount ? "active" : "";
    return `<span class="segment ${active} ${tone}"></span>`;
  }).join("");
}

function renderConnections(services) {
  const details = services?._details || {};
  $("#homeConnectionsList").innerHTML = CONNECTIONS
    .map(
      ({ key, label, icon }) => {
        const meta = details[key] || {};
        const connected = Boolean(services?.[key]);
        const stateLabel = String(meta.label || (connected ? "On" : "Off"));
        const state = String(meta.state || (connected ? "on" : "off")).toLowerCase();
        const cardClass = state === "reconnect" || state === "attention" ? "is-warning" : connected ? "is-on" : "is-off";
        return `
        <div class="connection-card ${cardClass}">
          <div class="connection-icon"><span data-lucide="${icon}" aria-hidden="true"></span></div>
          <div>
            <span>${label}</span>
            <strong>${escapeHtml(stateLabel)}</strong>
          </div>
          <span class="connection-switch" aria-hidden="true"><span></span></span>
        </div>
      `;
      }
    )
    .join("");
  refreshIcons($("#homeConnectionsList"));
}

function renderProactiveQueue(data = {}) {
  const top = Array.isArray(data.top) ? data.top : [];
  const changed = Array.isArray(data.changed) ? data.changed : [];
  if (!top.length) {
    const changedText = changed.length ? `<p class="subtle">${markdownish(changed.join(" "))}</p>` : "";
    return `<div class="empty-state compact">No urgent proactive items right now.</div>${changedText}`;
  }
  const cards = top.map((item, index) => {
    const score = Number(item.score || 0);
    const priority = String(item.priority || "medium").toUpperCase();
    const hint = item.action_hint ? `<p><strong>Next:</strong> ${markdownish(item.action_hint)}</p>` : "";
    const why = item.why ? `<p><strong>Why:</strong> ${markdownish(item.why)}</p>` : "";
    const date = item.event_date ? `<small>${markdownish(item.event_date)}</small>` : "";
    return `
      <article class="agenda-card">
        <div class="agenda-card-head">
          <strong>${index + 1}. ${markdownish(item.title || "H.I.R.A")}</strong>
          <span>${score} · ${priority}</span>
        </div>
        <p>${markdownish(item.body || "")}</p>
        ${why}
        ${hint}
        ${date}
      </article>
    `;
  }).join("");
  const changedNote = changed.length ? `<p class="subtle">${markdownish(changed.join(" "))}</p>` : "";
  return `${cards}${changedNote}`;
}

function renderMorningDigest(data = {}, { limit = 0 } = {}) {
  const items = Array.isArray(data.items) ? data.items : [];
  if (!items.length) {
    return `<div class="empty-state compact">No digest items returned yet.</div>`;
  }
  const visible = limit > 0 ? items.slice(0, limit) : items;
  const extra = limit > 0 ? Math.max(0, items.length - visible.length) : 0;
  const cards = visible.map((item, index) => {
    const meta = [item.label, item.source].filter(Boolean).join(" · ");
    const why = item.why ? `<p><strong>Why:</strong> ${markdownish(item.why)}</p>` : "";
    const title = markdownish(item.title || "Digest item");
    const safeLink = safeExternalLink(item.url);
    const link = safeLink ? `<p>${safeLink}</p>` : "";
    return `
      <article class="agenda-card digest-card">
        <div class="agenda-card-head">
          <strong>${index + 1}. ${title}</strong>
          ${meta ? `<span>${markdownish(meta)}</span>` : ""}
        </div>
        ${why}
        ${link}
      </article>
    `;
  }).join("");
  const more = extra ? `<div class="preview-more">+${extra} more in the full digest.</div>` : "";
  return `${cards}${more}`;
}

function minutesFromTime(value = "") {
  const match = String(value || "").match(/\b(\d{1,2}):(\d{2})\b/);
  if (!match) return null;
  return Number(match[1]) * 60 + Number(match[2]);
}

function minutesToTime(total) {
  const clean = Math.max(0, Math.min(23 * 60 + 59, Number(total || 0)));
  const hours = Math.floor(clean / 60);
  const minutes = clean % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

function timelineRelativeLabel(start, now) {
  if (!Number.isFinite(start) || !Number.isFinite(now)) return "";
  const diff = start - now;
  const abs = Math.abs(diff);
  const hours = Math.floor(abs / 60);
  const minutes = abs % 60;
  const text = hours ? `${hours}h ${minutes ? `${minutes}m` : ""}`.trim() : `${minutes}m`;
  if (diff > 0) return `In ${text}`;
  if (diff > -20) return "Now";
  return `${text} ago`;
}

function agendaTimelineItems(structured = {}) {
  const today = Array.isArray(structured.days) ? structured.days[0] : null;
  if (!today) return [];
  return [
    ...(today.lessons || []).map((item) => ({
      kind: "lesson",
      time: item.time || "Anytime",
      title: item.title || item.subject || "Lesson",
      meta: [item.subject, item.room].filter(Boolean).join(" · "),
      start: minutesFromTime(item.time),
    })),
    ...(today.events || []).map((item) => ({
      kind: "event",
      time: item.time || "Anytime",
      title: item.title || "Event",
      meta: item.meta || "",
      start: minutesFromTime(item.time),
    })),
    ...(today.due || []).map((item) => ({
      kind: "due",
      time: "Due",
      title: item.title || item.category || "Task due",
      meta: item.id ? `#${item.id}` : item.category || "",
      start: 23 * 60 + 30,
    })),
  ];
}

function prayerTimelineItems(prayers = {}) {
  const windowMinutes = Number(prayers.window_minutes || 20);
  return (prayers.prayers || [])
    .filter((item) => ["zohor", "asar", "maghrib", "isyak"].includes(item.key))
    .map((item) => {
      const start = minutesFromTime(item.time);
      const due = minutesFromTime(item.due_time);
      const end = Number.isFinite(due) && due > start ? due : start + windowMinutes;
      return {
        kind: "prayer",
        time: item.time || "--:--",
        title: `${item.label || "Prayer"} Window`,
        meta: `${item.time || "--:--"} - ${minutesToTime(end)}`,
        start,
        end,
      };
    });
}

function renderLivingTimeline(structured = {}, prayers = {}) {
  const now = minutesFromTime(prayers.now) ?? (new Date().getHours() * 60 + new Date().getMinutes());
  const allItems = [...agendaTimelineItems(structured), ...prayerTimelineItems(prayers)]
    .filter((item) => Number.isFinite(item.start))
    .sort((a, b) => a.start - b.start);
  state.homeTimelineItems = allItems;
  const visible = allItems
    .filter((item) => item.start >= now - 90)
    .slice(0, 7);
  const items = visible.length ? visible : allItems.slice(-3);
  if (!items.length) {
    return `<div class="empty-state compact">No timeline items for today yet.</div>`;
  }
  const iconMap = {
    due: "check-square",
    event: "calendar",
    lesson: "book-open",
    prayer: "sun",
  };
  return items
    .map((item) => {
      const relative = timelineRelativeLabel(item.start, now);
      const isNow = item.start <= now && (item.end || item.start + 20) >= now;
      return `
        <article class="timeline-item ${item.kind} ${isNow ? "is-now" : ""}">
          <div class="timeline-time">
            <strong>${markdownish(item.time)}</strong>
            ${relative ? `<span>${markdownish(relative)}</span>` : ""}
          </div>
          <div class="timeline-pin" aria-hidden="true"></div>
          <div class="timeline-copy">
            <span data-lucide="${iconMap[item.kind] || "circle"}" aria-hidden="true"></span>
            <div>
              <strong>${markdownish(item.title)}</strong>
              ${item.meta ? `<p>${markdownish(item.meta)}</p>` : ""}
            </div>
          </div>
        </article>
      `;
    })
    .join("");
}

function fileMemorySegments(text) {
  const count = countMeaningfulLines(text);
  if (!count) return 1;
  return Math.max(2, Math.min(12, Math.ceil(count / 2)));
}

function loadToneClass(tone) {
  return `load-${["green", "yellow", "orange", "red"].includes(tone) ? tone : "green"}`;
}

function renderDailyLoad(load = {}) {
  const today = load.today || {};
  const loadLabel = {
    green: "LOW LOAD",
    yellow: "STEADY LOAD",
    orange: "HEAVY LOAD",
    red: "CRITICAL LOAD",
  }[String(today.tone || "green").toLowerCase()] || "LOAD STATE";
  const toneClass = loadToneClass(today.tone);
  $("#dailyLoadTitle").textContent = today.label || "Today";
  $("#dailyLoadBadge").textContent = today.load || "Pretty chill";
  $("#dailyLoadBadge").className = `load-badge ${toneClass}`;
  const score = Number(today.score ?? 0);
  const scorePct = Math.max(0, Math.min(100, score));
  $("#dailyLoadScore").closest(".daily-load-score").className = `daily-load-score score-${String(today.tone || "green").toLowerCase()}`;
  $("#dailyLoadScore").closest(".daily-load-score").style.setProperty("--score-arc", `${scorePct * 2.7}deg`);
  $("#dailyLoadScore").textContent = String(today.score ?? 0);
  $("#dailyLoadScoreLabel").textContent = loadLabel;
  $("#dailyLoadLessons").textContent = String(today.lessons ?? 0);
  $("#dailyLoadEvents").textContent = String(today.events ?? 0);
  $("#dailyLoadDue").textContent = String(today.due ?? 0);
  $("#dailyLoadMarking").textContent = String(today.marking_scripts ?? 0);
  $("#dailyLoadNote").textContent = load.note || "Daily load will appear here.";
  $("#dailyLoadRestNote").textContent = load.rest_note || "Rest guidance will appear here.";
  $("#homeNextCount").textContent = String(today.lessons ?? 0);
  $("#homeNextLabel").textContent = today.lessons ? "LESSONS TODAY" : "NO LESSONS LISTED";
  $("#homeFocusValue").textContent = load.rest_note || load.note || "Standby";
  $("#homeFocusLabel").textContent = today.load ? `${String(today.load).toUpperCase()} DAY` : "LOAD GUIDANCE";
  renderSegments("#homeNextBar", Math.min(12, Math.max(1, Number(today.lessons || 0) * 3)), 12, Number(today.lessons || 0) ? "accent" : "muted");

  const days = Array.isArray(load.days) ? load.days.slice(0, state.homeDays) : [];
  $("#dailyLoadStrip").innerHTML = days.length
    ? days
        .map((day, index) => {
          const active = index === 0 ? "active" : "";
          const dayTone = loadToneClass(day.tone);
          return `
            <div class="load-day ${active}">
              <small>${markdownish(day.day_number || "")}</small>
              <span class="load-dot ${dayTone}" title="${escapeHtml(day.load || "")}"></span>
              <strong>${markdownish(day.label || "")}</strong>
            </div>
          `;
        })
        .join("")
    : "<div class='empty-state compact'>Load data unavailable.</div>";
  renderWorkloadTrend(load);
}

function sgtDateKey(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    timeZone: "Asia/Singapore",
  }).formatToParts(date);
  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${lookup.year}-${lookup.month}-${lookup.day}`;
}

function currentSgtMinutes() {
  const parts = new Intl.DateTimeFormat("en-SG", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "Asia/Singapore",
  }).formatToParts(new Date());
  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return Number(lookup.hour || 0) * 60 + Number(lookup.minute || 0);
}

function focusItemLabel(item) {
  if (!item) return { title: "Clear block", meta: "No current anchor" };
  const kind = String(item.kind || "item").toUpperCase();
  const title = item.title || "Untitled";
  const meta = [item.time, item.meta, kind].filter(Boolean).join(" · ");
  return { title, meta };
}

function renderTodayFocus(data = {}) {
  const now = currentSgtMinutes();
  const timeline = Array.isArray(state.homeTimelineItems) ? state.homeTimelineItems : [];
  const current = timeline.find((item) => Number.isFinite(item.start) && item.start <= now && (item.end || item.start + 20) >= now);
  const next = timeline.find((item) => Number.isFinite(item.start) && item.start > now);
  const fallbackNext = timeline.find((item) => Number.isFinite(item.start)) || null;
  const nowLabel = focusItemLabel(current);
  const nextLabel = focusItemLabel(next || fallbackNext);
  const todayKey = sgtDateKey();
  const taskItems = Array.isArray(data.tasks_structured?.items) ? data.tasks_structured.items : [];
  const structuredDue = Array.isArray(data.agenda_structured?.days?.[0]?.due) ? data.agenda_structured.days[0].due : [];
  const dueToday = taskItems.filter((item) => item.due === todayKey).length || structuredDue.length || Number(data.daily_load?.today?.due || 0);
  const markingLeft = Number(data.marking?.unmarked_scripts ?? data.daily_load?.today?.marking_scripts ?? 0);
  $("#focusNowTitle").textContent = nowLabel.title;
  $("#focusNowMeta").textContent = current ? nowLabel.meta : "Open time right now";
  $("#focusNextTitle").textContent = next ? nextLabel.title : nextLabel.title;
  $("#focusNextMeta").textContent = next ? nextLabel.meta : "Nothing later today";
  $("#focusDueCount").textContent = String(dueToday);
  $("#focusDueMeta").textContent = dueToday === 1 ? "task today" : "tasks today";
  $("#focusMarkingCount").textContent = String(markingLeft);
  const action = $("#focusActionBtn");
  if (action) {
    const lead = data.intelligence?.next_move || {};
    const title = lead.title || nextLabel.title || "today";
    const body = lead.body || nextLabel.meta || "";
    action.dataset.commandPrompt = `Help me execute the best next move now: ${title}. ${body}`.trim();
  }
  refreshIcons(document.querySelector(".today-focus-strip"));
}

function intelligenceSeverityClass(severity) {
  const clean = String(severity || "yellow").toLowerCase();
  return ["green", "yellow", "orange", "red"].includes(clean) ? clean : "yellow";
}

function renderIntelligenceList(items = [], emptyText = "No signal.") {
  if (!Array.isArray(items) || !items.length) {
    return `<div class="intelligence-item empty"><strong>${markdownish(emptyText)}</strong></div>`;
  }
  return items
    .slice(0, 4)
    .map((item) => {
      const severity = intelligenceSeverityClass(item.severity || "green");
      return `
        <article class="intelligence-item ${severity}">
          <strong>${markdownish(item.label || "Signal")}</strong>
          <p>${markdownish(item.detail || "")}</p>
        </article>
      `;
    })
    .join("");
}

function renderIntelligenceProtocol(protocol = {}) {
  const steps = Array.isArray(protocol.steps) ? protocol.steps : [];
  $("#intelligenceConfidence").textContent = `${protocol.confidence || "Limited"} confidence`;
  $("#intelligenceProtocolSteps").innerHTML = steps.length
    ? steps
        .slice(0, 3)
        .map((step) => `
          <article class="protocol-step">
            <div>
              <strong>${markdownish(step.phase || "Now")}</strong>
              <span>${markdownish(step.time || "")}</span>
            </div>
            <p>${markdownish(step.task || "")}</p>
          </article>
        `)
        .join("")
    : `<article class="protocol-step empty"><div><strong>Now</strong><span>0-20m</span></div><p>Waiting for a reliable operating signal.</p></article>`;
  const evidence = Array.isArray(protocol.evidence) ? protocol.evidence.filter(Boolean) : [];
  $("#intelligenceEvidence").innerHTML = evidence.length
    ? evidence.slice(0, 4).map((item) => `<span>${markdownish(item)}</span>`).join("")
    : "<span>No evidence yet</span>";
}

function renderForecastRadar(forecast = {}) {
  $("#intelligenceForecastHorizon").textContent = forecast.horizon || "7 days";
  const items = Array.isArray(forecast.items) ? forecast.items : [];
  $("#intelligenceForecast").innerHTML = items.length
    ? items
        .slice(0, 4)
        .map((item) => {
          const severity = intelligenceSeverityClass(item.severity || "green");
          return `
            <article class="forecast-item ${severity}">
              <div>
                <strong>${markdownish(item.label || "Forecast")}</strong>
                <span>${markdownish(item.when || "Soon")}</span>
              </div>
              <p>${markdownish(item.detail || "")}</p>
            </article>
          `;
        })
        .join("")
    : `<article class="forecast-item green"><div><strong>Stable horizon</strong><span>7d</span></div><p>No forecast signal yet.</p></article>`;
}

function renderAdaptivePlan(plan = {}) {
  const blocks = Array.isArray(plan.blocks) ? plan.blocks : [];
  $("#intelligencePlan").innerHTML = blocks.length
    ? blocks
        .slice(0, 4)
        .map((block) => `
          <article class="plan-block">
            <div>
              <strong>${markdownish(block.label || "Block")}</strong>
              <span>${markdownish(block.time || "")}</span>
            </div>
            <h4>${markdownish(block.title || "Execution block")}</h4>
            <p>${markdownish(block.detail || "")}</p>
          </article>
        `)
        .join("")
    : `<article class="plan-block"><div><strong>Prime</strong><span>Now</span></div><h4>Waiting</h4><p>No adaptive block yet.</p></article>`;
}

function trialStartDate() {
  const stored = localStorage.getItem("hira_trial_start_date");
  if (stored && !Number.isNaN(Date.parse(stored))) return stored;
  const today = new Date().toISOString().slice(0, 10);
  localStorage.setItem("hira_trial_start_date", today);
  return today;
}

function renderTrialLoop(trial = {}) {
  const start = new Date(`${trialStartDate()}T00:00:00`);
  const now = new Date();
  const day = Math.max(1, Math.min(7, Math.floor((now - start) / 86400000) + 1));
  $("#intelligenceTrialDay").textContent = `Day ${day} of 7`;
  $("#intelligenceTrialMetric").textContent = trial.metric || "Did H.I.R.A reduce decision friction today?";
  const checkpoints = Array.isArray(trial.checkpoints) ? trial.checkpoints : [];
  $("#intelligenceTrialCheckpoints").innerHTML = checkpoints.length
    ? checkpoints.slice(0, 3).map((item) => `<span>${markdownish(item)}</span>`).join("")
    : "<span>Follow the protocol once.</span><span>Log one correction.</span>";
  const button = $("#intelligenceTrialLogBtn");
  if (button && trial.review_prompt) {
    button.dataset.commandPrompt = trial.review_prompt;
  }
  refreshIcons(button);
}

function renderIntelligenceStack(intelligence = {}) {
  const readiness = Math.max(0, Math.min(100, Number(intelligence.readiness || 0)));
  const tone = intelligenceSeverityClass(intelligence.tone || "green");
  $("#intelligenceMode").textContent = intelligence.mode || "Standby";
  $("#intelligenceSignal").textContent = intelligence.signal || "Waiting for telemetry.";
  $("#intelligenceReadinessValue").textContent = String(Math.round(readiness));
  const readinessEl = $("#intelligenceReadiness");
  readinessEl.className = `intelligence-readiness score-${tone}`;
  readinessEl.style.setProperty("--score-arc", `${readiness * 2.7}deg`);
  const next = intelligence.next_move || {};
  $("#intelligenceNextTitle").textContent = next.title || "Protect a clean block";
  $("#intelligenceNextBody").textContent = next.body || "No critical signal is dominating right now.";
  renderIntelligenceProtocol(intelligence.protocol || {});
  renderForecastRadar(intelligence.forecast || {});
  renderAdaptivePlan(intelligence.adaptive_plan || {});
  renderTrialLoop(intelligence.trial || {});
  $("#intelligenceRisks").innerHTML = renderIntelligenceList(intelligence.risks, "No material risk.");
  $("#intelligenceOpportunities").innerHTML = renderIntelligenceList(intelligence.opportunities, "No opening yet.");
  const actions = Array.isArray(intelligence.actions) ? intelligence.actions : [];
  $("#intelligenceActions").innerHTML = actions.length
    ? actions
        .map((action) => `
          <button
            type="button"
            class="intelligence-action"
            data-command-action="${escapeHtml(action.action || "fill")}"
            data-command-prompt="${escapeHtml(action.prompt || "")}"
          >
            <span data-lucide="${escapeHtml(action.icon || "sparkles")}" aria-hidden="true"></span>
            <span>${markdownish(action.label || "Ask H.I.R.A")}</span>
          </button>
        `)
        .join("")
    : `<button type="button" class="intelligence-action" data-command-action="send" data-command-prompt="Build a fresh live H.I.R.A briefing for right now using the current Singapore date. Do not replay stored briefings."><span data-lucide="radar" aria-hidden="true"></span><span>Brief Me</span></button>`;
  refreshIcons($("#intelligenceActions"));
  refreshIcons(document.querySelector(".intelligence-next"));
}

function renderWorkloadTrend(load = {}) {
  const el = $("#workloadTrendChart");
  if (!el) return;
  const history = Array.isArray(load.previous_week) ? load.previous_week.slice(-5) : [];
  const future = Array.isArray(load.next_week) ? load.next_week.slice(0, 5) : [];
  const points = [...history, ...(load.today ? [{ ...load.today, label: "Now" }] : []), ...future];
  const scores = points.map((item) => Number(item.score || 0));
  if (!scores.length) {
    el.innerHTML = "<div class='empty-state compact'>No trend data yet.</div>";
    return;
  }

  const width = 360;
  const height = 150;
  const pad = 18;
  const usableWidth = width - pad * 2;
  const usableHeight = height - pad * 2;
  const maxScore = Math.max(100, ...scores);
  const minScore = 0;
  const coords = points.map((item, index) => {
    const x = pad + (points.length === 1 ? usableWidth / 2 : (index / (points.length - 1)) * usableWidth);
    const y = pad + usableHeight - ((Number(item.score || 0) - minScore) / (maxScore - minScore || 1)) * usableHeight;
    return { ...item, x, y };
  });
  const line = coords.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const average = Math.round(scores.reduce((sum, value) => sum + value, 0) / scores.length);
  const highest = Math.max(...scores);
  const lowest = Math.min(...scores);
  const highestDay = points[scores.indexOf(highest)]?.label || "";
  const lowestDay = points[scores.indexOf(lowest)]?.label || "";

  el.innerHTML = `
    <div class="trend-plot" role="img" aria-label="Workload trend from last five workdays through next five workdays">
      <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-hidden="true">
        <line x1="${pad}" y1="${pad}" x2="${pad}" y2="${height - pad}" class="trend-axis" />
        <line x1="${pad}" y1="${height - pad}" x2="${width - pad}" y2="${height - pad}" class="trend-axis" />
        <line x1="${pad}" y1="${pad + usableHeight * 0.25}" x2="${width - pad}" y2="${pad + usableHeight * 0.25}" class="trend-grid" />
        <line x1="${pad}" y1="${pad + usableHeight * 0.5}" x2="${width - pad}" y2="${pad + usableHeight * 0.5}" class="trend-grid" />
        <line x1="${pad}" y1="${pad + usableHeight * 0.75}" x2="${width - pad}" y2="${pad + usableHeight * 0.75}" class="trend-grid" />
        <polyline points="${line}" class="trend-line" />
        ${coords.map((point) => {
          const size = point.label === "Now" ? 6 : 4;
          return `<rect x="${(point.x - size / 2).toFixed(1)}" y="${(point.y - size / 2).toFixed(1)}" width="${size}" height="${size}" class="trend-dot ${loadToneClass(point.tone)}" />`;
        }).join("")}
      </svg>
      <div class="trend-labels">
        ${points.map((item) => `<span>${markdownish(item.label || "")}</span>`).join("")}
      </div>
    </div>
    <div class="trend-stats">
      <div><span>Average</span><strong>${average}</strong></div>
      <div><span>Highest${highestDay ? ` · ${markdownish(highestDay)}` : ""}</span><strong>${highest}</strong></div>
      <div><span>Lowest${lowestDay ? ` · ${markdownish(lowestDay)}` : ""}</span><strong>${lowest}</strong></div>
    </div>
  `;
}

function markingSegments(value, total) {
  if (!total || total <= 0) return 0;
  return Math.max(1, Math.min(12, Math.round((value / total) * 12)));
}

function renderMarkingSets(items = []) {
  if (!items.length) {
    return `
      <div class="marking-set empty">
        <span>All clear</span>
        <strong>0 active</strong>
      </div>
    `;
  }
  return items
    .slice(0, 5)
    .map((item) => {
      const total = Number(item.total_scripts || 0);
      const marked = Number(item.marked_scripts || 0);
      const unmarked = Number(item.unmarked_scripts || 0);
      const progress = item.progress_label || (total ? `${marked}/${total}` : `${marked} marked`);
      const segments = segmentMarkup(markingSegments(marked, total), 12, unmarked ? "accent" : "success");
      return `
        <div class="marking-set">
          <span>${markdownish(item.display_title || item.title || "Marking set")}</span>
          <strong>${markdownish(progress)}</strong>
          <div class="segment-bar marking-set-bar" aria-hidden="true">${segments}</div>
          <small>Total ${total || "unset"}${total ? ` · ${unmarked} left` : ""}</small>
        </div>
      `;
    })
    .join("");
}

function markingRailContext(marking = {}) {
  const items = Array.isArray(marking.sets) ? marking.sets : [];
  if (!items.length) return "ALL CLEAR";
  const item = items[0];
  const title = item.display_title || item.title || "Marking set";
  const progress = item.progress_label || `${Number(item.marked_scripts || 0)} marked`;
  const extra = items.length > 1 ? ` +${items.length - 1} more` : "";
  return `${title} - ${progress}${extra}`;
}

function renderClassOpsStatus(classops = {}) {
  const pending = Number(classops.pending_count || 0);
  const concerns = Number(classops.concern_count || 0);
  const dueNow = Number(classops.due_today_count || 0) + Number(classops.overdue_count || 0);
  $("#classOpsPendingValue").textContent = String(pending);
  $("#classOpsConcernValue").textContent = String(concerns);
  $("#classOpsDueValue").textContent = String(dueNow);
  const classes = Array.isArray(classops.classes) ? classops.classes : [];
  $("#classOpsStatusList").innerHTML = classes.length
    ? classes.slice(0, 4).map((item) => {
        const latest = item.latest_assignment || {};
        const title = latest.assignment_title || item.class_name || "Class";
        const submitted = Number(latest.submitted_count || 0);
        const roster = Number(latest.roster_count || item.roster_count || 0);
        const missing = Number(item.pending_count || latest.missing_count || 0);
        const due = latest.collect_by ? ` · due ${markdownish(latest.collect_by)}` : "";
        const insight = item.top_insight || {};
        const insightLine = insight.title ? `<small class="classops-insight">${markdownish(insight.title)}</small>` : "";
        return `
          <div class="classops-status-row" data-severity="${markdownish(insight.severity || (item.concern_count ? "watch" : "clear"))}">
            <span>${markdownish(item.class_name || "Class")}</span>
            <strong>${markdownish(title)}</strong>
            <small>${roster ? `${submitted}/${roster} submitted` : `${missing} pending`}${due}</small>
            ${insightLine}
          </div>
        `;
      }).join("")
    : `<div class="classops-status-row is-empty"><span>Ready</span><strong>No tracked submission gaps</strong><small>Open ClassOps to scan lessons and start tracking.</small></div>`;
}

function saveChatHistory() {
  localStorage.setItem("hira_pwa_chat", JSON.stringify(state.chatHistory.slice(-30)));
}

function saveChatNotificationIds() {
  state.chatNotificationIds = [...new Set(state.chatNotificationIds.map(String))].slice(-80);
  localStorage.setItem("hira_pwa_chat_notification_ids", JSON.stringify(state.chatNotificationIds));
}

function scrollMessagesToBottom() {
  const messages = $("#messages");
  if (!messages) return;
  messages.scrollTop = messages.scrollHeight;
}

function stopSpeechPlayback() {
  if (state.speechAudio) {
    state.speechAudio.pause();
    state.speechAudio.src = "";
  }
  if (state.speechUrl) {
    URL.revokeObjectURL(state.speechUrl);
    state.speechUrl = "";
  }
}

async function playSpeech(text, control = null) {
  const clean = visibleChatText(text || "", { final: true }).trim();
  if (!clean) return;
  stopSpeechPlayback();
  const previous = control?.innerHTML || "";
  if (control) {
    control.disabled = true;
    control.innerHTML = `<span data-lucide="loader-2" aria-hidden="true"></span>`;
    refreshIcons(control);
  }
  try {
    const response = await fetchWithToken("/api/tts", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ text: clean }),
    });
    const blob = await response.blob();
    state.speechUrl = URL.createObjectURL(blob);
    state.speechAudio = new Audio(state.speechUrl);
    state.speechAudio.addEventListener("ended", stopSpeechPlayback, { once: true });
    await state.speechAudio.play();
    setStatus("Speaking reply.", "ok");
  } catch (error) {
    console.error(error);
    setStatus(`Could not speak that: ${error.message}`, "warn");
  } finally {
    if (control) {
      control.disabled = false;
      control.innerHTML = previous || `<span data-lucide="volume-2" aria-hidden="true"></span>`;
      refreshIcons(control);
    }
  }
}

function maybeAutoSpeak(text) {
  if (!state.autoSpeak) return;
  playSpeech(text).catch(() => {});
}

function messageSpeakControl(text = "") {
  const clean = visibleChatText(text || "", { final: true }).trim();
  return `
    <button type="button" class="message-speak-btn" data-speak-message ${clean ? "" : "hidden"} title="Speak reply" aria-label="Speak reply" data-speak-text="${escapeHtml(clean)}">
      <span data-lucide="volume-2" aria-hidden="true"></span>
    </button>
  `;
}

function escapeHtml(text) {
  return (text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function safeHttpUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  try {
    const parsed = new URL(raw, window.location.href);
    if (!["http:", "https:"].includes(parsed.protocol)) return "";
    return parsed.href;
  } catch (_) {
    return "";
  }
}

function safeExternalLink(url, label = "Read source") {
  const safeUrl = safeHttpUrl(url);
  if (!safeUrl) return "";
  return `<a href="${escapeHtml(safeUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
}

function notificationKindClass(kind) {
  const clean = String(kind || "notice").trim().toLowerCase();
  return ["notice", "briefing", "reminder", "update", "test"].includes(clean) ? clean : "notice";
}

function markdownish(text) {
  const codeSpans = [];
  const protectedText = String(text || "").replace(/`([^`]+)`/g, (_match, code) => {
    const index = codeSpans.length;
    codeSpans.push(`<code>${escapeHtml(code)}</code>`);
    return `\u0000CODE${index}\u0000`;
  });
  return escapeHtml(protectedText)
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/\*([^*]+)\*/g, "<strong>$1</strong>")
    .replace(/_([^_]+)_/g, "<em>$1</em>")
    .replace(/\*/g, "")
    .replace(/\u0000CODE(\d+)\u0000/g, (_match, index) => codeSpans[Number(index)] || "");
}

function stripCitationMarkers(text) {
  return String(text || "")
    .replace(/[\uE000-\uF8FF\uFFFD]cite(?:[\uE000-\uF8FF\uFFFD][\w.-]+)+[\uE000-\uF8FF\uFFFD]?/g, "")
    .replace(/[\uE000-\uF8FF\uFFFD]cite(?:[\uE000-\uF8FF\uFFFD]?[\w.-]*)?$/g, "")
    .replace(/(?:^|[ \t])(?:turn\d+(?:search|news|view|open|fetch)\d+[\uE000-\uF8FF\uFFFD]?)(?=[ \t\n.,;:!?]|$)/g, " ")
    .replace(/【\s*\d+(?::\d+)?\s*†[^】]*】/g, "")
    .replace(/【\s*source\s*】/gi, "")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/[ \t]+([.,;:!?])/g, "$1")
    .replace(/\n[ \t]+/g, "\n")
    .trim();
}

function stripSourcePlumbingUrls(text) {
  const rawLines = String(text || "").split("\n");
  const lines = rawLines
    .map((line, index) => {
      let clean = line.replace(SOURCE_PLUMBING_URL_PATTERN, "").trimEnd();
      const nextLine = String(rawLines[index + 1] || "").trim();
      if (!clean.trim()) return "";
      if (/https?:\/\//i.test(clean)) return "";
      if (/^\s*(?:[-*•]\s*)?(?:source|sources|references|links|evidence|receipts)\s*:/i.test(clean)) return "";
      if (
        /^https?:\/\//i.test(nextLine) &&
        /\b(?:source|official|calendar|report|scoreboard|probe|article|coverage)\b/i.test(clean)
      ) {
        return "";
      }
      if (/^\s*(?:[-*•]\s*)?(?:source|sources|link|links)\s*:?\s*$/i.test(clean)) return "";
      if (/\b(?:source|sources|report|calendar|scoreboard|probe|live brief)\b/i.test(clean)) {
        clean = clean.replace(/:\s*$/, "");
      }
      return clean;
    })
    .filter(Boolean);
  return lines.join("\n").replace(/\n{3,}/g, "\n\n").trim();
}

function renderTextBlock(text) {
  return (text || "")
    .split("\n")
    .map((line) => {
      if (!line.trim()) return "<div class='spacer'></div>";
      return `<div>${markdownish(line)}</div>`;
    })
    .join("");
}

function renderChatText(text) {
  const blocks = [];
  let openList = false;
  for (const raw of (text || "").split("\n")) {
    const line = raw.trim();
    if (!line) {
      if (openList) {
        blocks.push("</ul>");
        openList = false;
      }
      blocks.push("<div class='spacer'></div>");
      continue;
    }
    const bullet = line.match(/^(?:[-•]|\d+[.)])\s*(.+)$/);
    if (bullet) {
      if (!openList) {
        blocks.push("<ul class='chat-list'>");
        openList = true;
      }
      blocks.push(`<li>${markdownish(bullet[1])}</li>`);
      continue;
    }
    if (openList) {
      blocks.push("</ul>");
      openList = false;
    }
    blocks.push(`<p>${markdownish(line)}</p>`);
  }
  if (openList) blocks.push("</ul>");
  return blocks.join("");
}

function normaliseQuotedJson(text) {
  return String(text || "")
    .trim()
    .replace(/[“”]/g, "\"")
    .replace(/[‘’]/g, "'");
}

function objectHasInternalToolShape(value) {
  if (!value || typeof value !== "object") return false;
  const keys = Object.keys(value).map((key) => key.toLowerCase());
  const compactKeys = keys.map((key) => key.replace(/[^a-z0-9]/g, ""));
  const joined = keys.join(" ");
  const internalSignals = [
    "avoid_keywords",
    "duration_minutes",
    "window_start",
    "window_end",
    "tool_call",
    "tool_name",
    "arguments",
    "max_items",
    "maxitems",
  ];
  const hasToolSignal = internalSignals.some((signal) => joined.includes(signal));
  const hasSearchToolBundle =
    (compactKeys.includes("query") || compactKeys.includes("q")) &&
    (compactKeys.includes("maxitems") || compactKeys.includes("account") || compactKeys.includes("maxsources"));
  const hasSchedulingBundle =
    keys.includes("days") &&
    (keys.includes("purpose") || keys.includes("duration_minutes") || keys.includes("window_start"));
  const hasReminderBundle =
    compactKeys.includes("description") &&
    compactKeys.some((key) => key === "duedate" || key === "due") &&
    (compactKeys.includes("category") || /teaching|cca|gameplan|ruh|personal/i.test(String(value.category || "")));
  const hasMarkingBundle =
    compactKeys.includes("title") &&
    (compactKeys.includes("totalscripts") || compactKeys.includes("stackcount") || compactKeys.includes("collecteddate"));
  return hasToolSignal || hasSearchToolBundle || hasSchedulingBundle || hasReminderBundle || hasMarkingBundle;
}

function isInternalToolPayload(text) {
  const clean = normaliseQuotedJson(text).replace(/^H:\s*/i, "").trim();
  if (!clean || clean.length > 1400) return false;
  if (!/^[\[{]/.test(clean)) return false;
  try {
    const parsed = JSON.parse(clean);
    const items = Array.isArray(parsed) ? parsed : [parsed];
    if (items.length && items.every(objectHasInternalToolShape)) return true;
  } catch (_) {
    // Some model/tool payloads arrive with smart quotes or partial formatting; use
    // a conservative shape check so those do not leak into the chat transcript.
  }
  const jsonLines = clean.split(/\n+/).map((line) => line.trim()).filter(Boolean);
  if (jsonLines.length > 1 && jsonLines.every((line) => line.startsWith("{") && line.endsWith("}"))) {
    try {
      const items = jsonLines.map((line) => JSON.parse(line));
      if (items.every(objectHasInternalToolShape)) return true;
    } catch (_) {
      // Fall through to regex checks below.
    }
  }
  return (
    /"?(?:duration_minutes|window_start|window_end|avoid_keywords)"?\s*:/.test(clean) ||
    /"?(?:due_date|due date|duedate)"?\s*:/.test(clean) ||
    (/"?query"?\s*:/.test(clean) && /"?(?:max_items|maxitems|account|max_sources)"?\s*:/.test(clean)) ||
    (/"?days"?\s*:/.test(clean) && /"?(?:purpose|window_start)"?\s*:/.test(clean))
  );
}

function visibleChatText(text, { final = false } = {}) {
  const clean = String(text || "").trim();
  if (!clean) return "";
  if (isInternalToolPayload(clean)) return final ? INTERNAL_TOOL_FALLBACK : "";
  return stripSourcePlumbingUrls(stripCitationMarkers(text));
}

function cleanStoredChatHistory(items = []) {
  return items
    .map((item) => {
      if (!item || typeof item !== "object") return null;
      const text = item.role === "hira" ? visibleChatText(item.text || "", { final: true }) : String(item.text || "");
      if (!text.trim()) return null;
      return CHAT_DEBUG_TRACE && item.trace ? { ...item, text } : { role: item.role, text };
    })
    .filter(Boolean)
    .slice(-30);
}

function renderAgendaCards(text, { limit = 0 } = {}) {
  const lines = (text || "")
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return "<div class='empty-state'>No agenda items found.</div>";

  const cards = [];
  let currentDay = "";
  for (const raw of lines) {
    const line = raw.replace(/^[•\-*]\s*/, "").trim();
    const plainLine = line.replace(/[*_`]/g, "").trim();
    if (/^agenda$/i.test(plainLine) || /^google not connected/i.test(plainLine)) continue;
    if (/\bSGT$/i.test(plainLine) && /\b\d{4}\b/.test(plainLine)) continue;
    const dayMatch = line.match(/^(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday|\d{1,2}[\/\-.]\d{1,2}|\d{4}-\d{2}-\d{2})\b[:\s-]*/i);
    const headingMatch = plainLine.match(/^(today|tomorrow|monday|tuesday|wednesday|thursday|friday|saturday|sunday|.+\b\d{4}\b).*/i);
    if ((dayMatch || headingMatch) && plainLine.length < 90 && !plainLine.match(/\b\d{1,2}:\d{2}\b/)) {
      currentDay = plainLine.replace(/[:\-]\s*$/, "");
      continue;
    }
    const timeMatch = line.match(/(\b\d{1,2}(?::\d{2})?\s*(?:am|pm)?\b\s*(?:[-–—to]+\s*\d{1,2}(?::\d{2})?\s*(?:am|pm)?)?)/i);
    const time = timeMatch ? timeMatch[1].replace(/\s+/g, " ").trim() : "Anytime";
    const remainder = line.replace(timeMatch?.[1] || "", "").replace(/^[:\-–\s]+/, "").trim() || line;
    const [titlePart, locationPart] = remainder.split(/\s+[·|]\s+/);
    cards.push(`
      <article class="agenda-card">
        <div class="agenda-time">${markdownish(time)}</div>
        <div class="agenda-copy">
          <p class="agenda-day">${markdownish(currentDay || "Today at school")}</p>
          <strong>${markdownish(titlePart || remainder)}</strong>
          ${locationPart ? `<span>${markdownish(locationPart)}</span>` : ""}
        </div>
      </article>
    `);
  }

  if (!cards.length) return renderTextBlock(text);
  const visible = limit > 0 ? cards.slice(0, limit) : cards;
  const extra = limit > 0 ? Math.max(0, cards.length - visible.length) : 0;
  const more = extra ? `<div class="preview-more">+${extra} more in Agenda.</div>` : "";
  return `<div class="agenda-list">${visible.join("")}${more}</div>`;
}

function renderAgendaStructured(data) {
  const days = data?.days || [];
  if (!days.length) return "<div class='empty-state'>No agenda items found.</div>";
  return `
    <div class="agenda-day-list">
      ${days
        .map((day) => {
          const items = [
            ...(day.lessons || []).map((item) => ({ ...item, label: item.subject, meta: item.room })),
            ...(day.events || []).map((item) => ({ ...item, label: "Event" })),
            ...(day.due || []).map((item) => ({ ...item, time: "Due", label: item.category || "Task", meta: item.id ? `#${item.id}` : "" })),
          ];
          const context = [day.label, day.week].filter(Boolean).join(" · ") || "Calendar day";
          return `
            <section class="agenda-day-group">
              <div class="agenda-day-header">
                <div>
                  <h3>${markdownish(day.label)}</h3>
                  <p>${markdownish(day.week || "Calendar day")}</p>
                </div>
                <strong>${items.length}</strong>
              </div>
              ${
                items.length
                  ? `<div class="agenda-list">${items
                      .map(
                        (item) => `
                          <article class="agenda-card ${item.kind || ""}">
                            <div class="agenda-time">${markdownish(item.time || "Anytime")}</div>
                            <div class="agenda-copy">
                              <p class="agenda-day">${markdownish(context)}</p>
                              <strong>${markdownish(item.title || item.label || "")}</strong>
                              ${item.meta ? `<span>${markdownish(item.meta)}</span>` : ""}
                            </div>
                          </article>
                        `
                      )
                      .join("")}</div>`
                  : "<div class='empty-state compact'>No lessons, events, or due items.</div>"
              }
            </section>
          `;
        })
        .join("")}
    </div>
  `;
}

function renderTaskList(data, heading = "Task Brief · Now to 7 May", { limit = 0 } = {}) {
  const items = data?.items || [];
  if (!items.length) return "<div class='empty-state'>No active tasks in that window.</div>";
  const visible = limit > 0 ? items.slice(0, limit) : items;
  const extra = limit > 0 ? Math.max(0, items.length - visible.length) : 0;
  return `
    <div class="task-brief-card">
      <div class="task-brief-head">${markdownish(heading)}</div>
      <div class="task-list">
      ${visible
        .map((item) => {
          const due = item.due || item.weekday || "No due date";
          const meta = [item.category, item.priority, item.effort].filter(Boolean).join(" / ");
          return `
            <article class="task-item ${item.overdue ? "overdue" : ""}" data-task-id="${markdownish(item.id)}">
              <label class="task-check" title="Mark done">
                <input type="checkbox" data-task-done="${markdownish(item.id)}" />
                <span></span>
              </label>
              <div class="task-num">${markdownish(item.id)}</div>
              <div class="task-copy">
                <div class="task-date">${markdownish(due)}</div>
                <p>${markdownish(item.description || "")}</p>
                ${meta ? `<small>${markdownish(meta)}</small>` : ""}
                <div class="task-actions">
                  <button type="button" class="ghost-btn" data-task-done-button="${markdownish(item.id)}">
                    <span data-lucide="check" aria-hidden="true"></span>
                    Done
                  </button>
                  <button type="button" class="ghost-btn" data-task-snooze="${markdownish(item.id)}">
                    <span data-lucide="alarm-clock" aria-hidden="true"></span>
                    Tonight
                  </button>
                  <button type="button" class="ghost-btn" data-task-plan="${markdownish(item.id)}">
                    <span data-lucide="sparkles" aria-hidden="true"></span>
                    Plan
                  </button>
                </div>
              </div>
            </article>
          `;
        })
        .join("")}
        ${extra ? `<div class="preview-more">+${extra} more in Tasks.</div>` : ""}
      </div>
    </div>
  `;
}

function renderTaskBriefFromText(text, options = {}) {
  const lines = (text || "")
    .split("\n")
    .map((line) => line.replace(/^[•\-*]\s*/, "").trim())
    .filter(Boolean);
  const items = [];
  for (const line of lines) {
    const plain = line.replace(/[*_`]/g, "").trim();
    if (/^(task brief|tasks?|no active tasks)/i.test(plain)) continue;
    const match = plain.match(/^(?:#|\[)?(\d+)(?:\])?\s*(?:[:\-–]\s*)?(?:(\d{4}-\d{2}-\d{2})\s*[:\-–]\s*)?(.*)$/);
    if (!match) continue;
    items.push({ id: match[1], due: match[2] || "", description: match[3] || "" });
  }
  if (!items.length) return renderTextBlock(text);
  return renderTaskList({ items }, "Task Brief · Now to 7 May", options);
}

async function completeTask(taskId, control) {
  if (control) control.disabled = true;
  const taskItem = control?.closest?.(".task-item") || document.querySelector(`.task-item[data-task-id="${CSS.escape(String(taskId || ""))}"]`);
  const desc = taskItem?.querySelector(".task-copy p")?.textContent?.trim() || "";
  try {
    const data = await api(`/api/tasks/${encodeURIComponent(taskId)}/done`, { method: "POST", headers: headers(false) });
    taskItem?.classList.add("completed");
    // Glyph flash — single sweep to confirm completion
    const glyph = document.getElementById("topbarGlyph");
    if (glyph) {
      glyph.classList.add("is-flash");
      setTimeout(() => glyph.classList.remove("is-flash"), 950);
    }
    const shortDesc = desc ? `"${desc.slice(0, 52)}${desc.length > 52 ? "…" : ""}"` : `#${taskId}`;
    setStatus(`Done ✓ ${shortDesc}`, "ok");
    const syncedMarking = data?.synced_marking?.title;
    const chatAck = desc
      ? `Done - cleared "${desc}" from your task list.${syncedMarking ? ` Also closed the matching marking stack: ${syncedMarking}.` : ""}`
      : `Done - cleared task #${taskId}.${syncedMarking ? ` Also closed the matching marking stack: ${syncedMarking}.` : ""}`;
    addMessage("hira", chatAck);
    // Give the fade animation a moment before refreshing
    setTimeout(async () => {
      await loadHome({ force: true, background: true, useCache: false });
      if ($("#tasksView")?.classList.contains("active") || state.currentView === "tasks") {
        await loadTasks(Number($("#tasksDays")?.value || 30));
      }
    }, 500);
  } catch (error) {
    if (control?.type === "checkbox") control.checked = false;
    if (control) control.disabled = false;
    setStatus(error.message, "error");
  }
}

function addMessage(role, text, persist = true) {
  const visibleText = role === "hira" ? visibleChatText(text, { final: true }) : String(text || "");
  const el = document.createElement("article");
  el.className = `message ${role}`;
  el.innerHTML = `<div class="message-body">${renderChatText(visibleText)}</div>${role === "hira" ? messageSpeakControl(visibleText) : ""}`;
  $("#messages").appendChild(el);
  refreshIcons(el);
  scrollMessagesToBottom();
  if (persist) {
    state.chatHistory.push({ role, text: visibleText });
    state.chatHistory = state.chatHistory.slice(-30);
    saveChatHistory();
    updateChatChrome();
  }
  return el;
}

function traceValue(value) {
  if (Array.isArray(value)) return value.filter(Boolean).join(", ") || "-";
  if (value && typeof value === "object") return JSON.stringify(value);
  return String(value || "-");
}

function renderTrace(el, trace) {
  if (!CHAT_DEBUG_TRACE) return;
  if (!el || !trace || typeof trace !== "object") return;
  let panel = el.querySelector(".chat-trace");
  if (!panel) {
    panel = document.createElement("details");
    panel.className = "chat-trace";
    const summary = document.createElement("summary");
    summary.className = "chat-trace-summary";
    const body = document.createElement("div");
    body.className = "chat-trace-body";
    panel.append(summary, body);
    el.appendChild(panel);
  }
  const summary = panel.querySelector(".chat-trace-summary");
  const body = panel.querySelector(".chat-trace-body");
  const route = trace.route || "pending";
  const mode = trace.final_mode || "running";
  const gate = trace.confidence_gate || "pending";
  summary.textContent = `Trace · ${route} · ${gate} · ${mode}`;

  const contracts = Array.isArray(trace.source_contracts_seen) ? trace.source_contracts_seen : [];
  const memorySources = Array.isArray(trace.memory_sources) ? trace.memory_sources : [];
  const modelPolicy = trace.model_policy && typeof trace.model_policy === "object" ? trace.model_policy : {};
  const threadState = trace.thread_state && typeof trace.thread_state === "object" ? trace.thread_state : {};
  const responseContract = trace.response_contract && typeof trace.response_contract === "object" ? trace.response_contract : {};
  const nativeEvents = Array.isArray(trace.openai_native_tool_events) ? trace.openai_native_tool_events : [];
  const nativeObservations = Array.isArray(trace.openai_native_observations) ? trace.openai_native_observations : [];
  const openaiCitations = Array.isArray(trace.openai_citations) ? trace.openai_citations : [];
  const rows = [
    ["Route", route],
    ["Model", modelPolicy.model || "-"],
    ["Tier", modelPolicy.tier || "-"],
    ["Specialist", modelPolicy.specialist || "-"],
    ["Reasoning", modelPolicy.reasoning_effort || "-"],
    ["Native tools", modelPolicy.native_tools || []],
    ["Native events", nativeEvents],
    ["Stateful", trace.openai_stateful ? "yes" : modelPolicy.stateful ? "ready" : "-"],
    ["Response ID", trace.openai_response_id || "-"],
    ["Forced tool", trace.forced_tool || "-"],
    ["Tools available", trace.tools_available || []],
    ["Tools called", trace.tools_called || []],
    ["Thread", threadState.is_followup ? "follow-up" : "standalone"],
    ["Topics", threadState.topic_signals || []],
    ["Confidence gate", gate],
    ["Contract", responseContract.status || "-"],
    ["Final mode", mode],
    ["Error phase", trace.error_phase || "-"],
  ];
  body.innerHTML = "";
  for (const [label, value] of rows) {
    const row = document.createElement("div");
    row.className = "chat-trace-row";
    const key = document.createElement("span");
    key.textContent = label;
    const val = document.createElement("strong");
    val.textContent = traceValue(value);
    row.append(key, val);
    body.appendChild(row);
  }
  if (contracts.length) {
    const section = document.createElement("div");
    section.className = "chat-trace-contracts";
    const heading = document.createElement("span");
    heading.textContent = "Source contracts";
    section.appendChild(heading);
    for (const contract of contracts) {
      const item = document.createElement("p");
      item.textContent = `${contract.status || "unknown"} · ${contract.as_of || "no date"} · ${contract.source || "source"} · ${contract.reason || ""}`;
      section.appendChild(item);
    }
    body.appendChild(section);
  }
  if (nativeObservations.length || openaiCitations.length) {
    const section = document.createElement("div");
    section.className = "chat-trace-contracts";
    const heading = document.createElement("span");
    heading.textContent = "OpenAI native tools";
    section.appendChild(heading);
    for (const observation of nativeObservations.slice(0, 4)) {
      const item = document.createElement("p");
      item.textContent = `${observation.type || "tool"} · ${observation.status || "observed"} · ${traceValue(observation.queries || observation.action || observation.results || observation.outputs || "")}`;
      section.appendChild(item);
    }
    for (const citation of openaiCitations.slice(0, 4)) {
      const item = document.createElement("p");
      item.textContent = `${citation.title || citation.type || "citation"} · ${citation.url || ""}`;
      section.appendChild(item);
    }
    body.appendChild(section);
  }
  if (memorySources.length) {
    const section = document.createElement("div");
    section.className = "chat-trace-contracts";
    const heading = document.createElement("span");
    heading.textContent = "Memory sources";
    section.appendChild(heading);
    for (const source of memorySources.slice(0, 5)) {
      const item = document.createElement("p");
      item.textContent = `${source.category || "memory"} · score ${source.score || 0} · ${source.text || ""}`;
      section.appendChild(item);
    }
    body.appendChild(section);
  }
  scrollMessagesToBottom();
}

function setHiraSpeaking(el, speaking) {
  el?.classList.toggle("speaking", Boolean(speaking));
  const signal = $("#hiraSignal");
  signal?.classList.toggle("is-speaking", Boolean(speaking));
  const label = $("#hiraSignalState");
  if (label) label.textContent = "Live";
  // Glyph strip — cascade when HIRA is processing, breathe at idle
  document.getElementById("topbarGlyph")?.classList.toggle("is-active", Boolean(speaking));
  if (speaking) {
    if (glyphMode !== "chat") glyphModeBeforeChat = glyphMode;
    renderNothingGlyph("chat");
  } else {
    renderNothingGlyph(glyphModeBeforeChat);
  }
}

function updateMessage(el, text) {
  const clean = visibleChatText(text || "") || "";
  el.querySelector(".message-body").innerHTML = renderChatText(clean);
  const speakButton = el.querySelector("[data-speak-message]");
  if (speakButton) {
    const speakText = visibleChatText(clean, { final: true }).trim();
    speakButton.hidden = !speakText;
    speakButton.dataset.speakText = speakText;
  }
  refreshIcons(el);
  scrollMessagesToBottom();
}

function progressTextForTool(name = "") {
  const clean = String(name || "").trim();
  if (["get_latest_news", "get_liverpool_brief", "get_f1_brief", "web_search", "web_research", "fetch_url"].includes(clean)) {
    return [
      "Still checking live sources...",
      "Reading and cross-checking the useful bits...",
      "This is taking longer than it should. I’m still on it.",
    ];
  }
  if (["get_gmail_brief", "create_gmail_draft"].includes(clean)) {
    return [
      "Still checking Gmail...",
      "Reading the matching messages...",
      "This is taking longer than it should. I’m still on it.",
    ];
  }
  if (["get_timetable", "get_assistant_context", "get_task_brief", "get_cca_schedule", "get_classops_brief"].includes(clean)) {
    return [
      "Still checking your current context...",
      "Putting the relevant details together...",
      "This is taking longer than it should. I’m still on it.",
    ];
  }
  return [
    "Still working...",
    "Putting the answer together...",
    "This is taking longer than it should. I’m still on it.",
  ];
}

function startChatProgress(pending, getBaseText, getToolName) {
  const timers = [];
  const delays = [4500, 12000, 24000];
  delays.forEach((delay, index) => {
    timers.push(window.setTimeout(() => {
      if (!state.chatBusy || !pending?.isConnected) return;
      const baseText = visibleChatText(getBaseText?.() || "");
      const notes = progressTextForTool(getToolName?.() || "");
      const note = notes[Math.min(index, notes.length - 1)];
      if (!note) return;
      updateMessage(pending, [baseText, note].filter(Boolean).join("\n\n"));
    }, delay));
  });
  return () => timers.forEach((timer) => window.clearTimeout(timer));
}

function appendToolStatus(el, name) {
  const labels = {
    create_calendar_event: "Adding to calendar...",
    delete_calendar_event_by_text: "Checking your calendar...",
    add_reminder: "Saving a reminder...",
    get_assistant_context: "Checking your day...",
    get_timetable: "Checking the timetable...",
    get_task_brief: "Checking tasks...",
    get_classops_brief: "Checking ClassOps...",
    get_gmail_brief: "Checking Gmail...",
    create_gmail_draft: "Drafting email...",
    get_nea_weather: "Checking NEA weather...",
    get_muis_prayer_times: "Checking MUIS prayer times...",
    get_latest_news: "Checking latest news...",
    get_liverpool_brief: "Checking Liverpool...",
    get_f1_brief: "Checking F1...",
    web_search: "Searching...",
    fetch_url: "Reading link...",
  };
  setStatus(labels[name] || "Working in the background...", "muted");
  if (!CHAT_DEBUG_TRACE) return;
  const status = document.createElement("div");
  status.className = "tool-status";
  status.innerHTML = `<span data-lucide="loader-2" aria-hidden="true"></span>${labels[name] || "Using a tool..."}`;
  el.appendChild(status);
  refreshIcons(status);
  scrollMessagesToBottom();
}

function renderUnderstanding(el, understanding) {
  if (!CHAT_DEBUG_TRACE) return;
  if (!el || !understanding) return;
  let cue = el.querySelector(".understanding-cue");
  if (!cue) {
    cue = document.createElement("div");
    cue.className = "understanding-cue";
    el.appendChild(cue);
  }
  const parts = [];
  if (understanding.subject) parts.push(`Tracking: ${understanding.subject}`);
  if (understanding.action) parts.push(`Action: ${understanding.action}`);
  if (understanding.conflict) parts.push(`Older: ${understanding.conflict}`);
  cue.textContent = parts.join(" | ");
  scrollMessagesToBottom();
}

function clearToolStatuses(el) {
  el?.querySelectorAll(".tool-status").forEach((item) => item.remove());
}

function chatNeedsDeviceLocation(message = "") {
  return /\b(location|where|journey|travel|route|directions|commute|drive|driving|mrt|bus|walk|walking|masjid|mosque|nearby|near me)\b/i.test(message);
}

function cachedDeviceLocation(maxAgeMs = 10 * 60 * 1000) {
  const cached = state.deviceLocation;
  if (!cached?.timestamp) return null;
  const age = Date.now() - Date.parse(cached.timestamp);
  return Number.isFinite(age) && age >= 0 && age <= maxAgeMs ? cached : null;
}

async function getDeviceLocationForChat(message = "") {
  if (!chatNeedsDeviceLocation(message) || !navigator.geolocation) {
    return cachedDeviceLocation();
  }
  const cached = cachedDeviceLocation(2 * 60 * 1000);
  if (cached) return cached;
  try {
    const position = await new Promise((resolve, reject) => {
      navigator.geolocation.getCurrentPosition(resolve, reject, {
        enableHighAccuracy: true,
        maximumAge: 60 * 1000,
        timeout: 5000,
      });
    });
    const location = {
      lat: position.coords.latitude,
      lon: position.coords.longitude,
      accuracy: position.coords.accuracy,
      timestamp: new Date(position.timestamp || Date.now()).toISOString(),
    };
    state.deviceLocation = location;
    localStorage.setItem("hira_pwa_device_location", JSON.stringify(location));
    return location;
  } catch (error) {
    console.info("Device location unavailable for chat", error?.message || error);
    return cachedDeviceLocation();
  }
}

async function streamChatResponse(message, onEvent) {
  const location = await getDeviceLocationForChat(message);
  const response = await fetchWithToken("/api/chat", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ message, location }),
  });
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("text/event-stream")) {
    const data = await response.json();
    const fallback = "I didn’t receive a usable H.I.R.A response. Try again in a moment.";
    onEvent({ type: "text", text: data.reply || fallback });
    onEvent({ type: "done", text: data.reply || fallback });
    return data.reply || fallback;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let finalText = "";
  let streamedText = "";

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const events = buffer.split("\n\n");
    buffer = events.pop() || "";
    for (const raw of events) {
      const line = raw.split("\n").find((item) => item.startsWith("data: "));
      if (!line) continue;
      let event;
      try {
        event = JSON.parse(line.slice(6));
      } catch (_) {
        continue;
      }
      if (event.type === "text") streamedText += event.text || "";
      if (event.type === "replace") streamedText = event.text || "";
      if (event.type === "done") finalText = event.text || streamedText;
      onEvent(event, streamedText);
      if (event.type === "error") {
        throw new Error(event.message || "H.I.R.A hit a backend snag. Try again in a moment.");
      }
    }
  }
  return finalText || streamedText || "I didn’t receive a usable H.I.R.A response. Try again in a moment.";
}

function renderStoredChat() {
  $("#messages").innerHTML = "";
  for (const item of state.chatHistory) {
    const el = addMessage(item.role, item.text, false);
    if (item.trace) renderTrace(el, item.trace);
  }
  updateChatChrome();
}

function updateChatChrome() {
  const hasChat = state.chatHistory.length > 0;
  $("#resetChatBtn").hidden = !hasChat;
  document.querySelector(".chat-main")?.classList.toggle("chat-empty", !hasChat);
}

function mountChatInHome() {
  const mount = $("#homeChatMount");
  const chat = document.querySelector(".chat-shell");
  if (mount && chat && chat.parentElement !== mount) mount.appendChild(chat);
}

function applyHomeSectionDismissals() {
  const dismissed = new Set(state.dismissedHomeSections.map(String));
  document.querySelectorAll("[data-home-section]").forEach((section) => {
    section.hidden = dismissed.has(section.dataset.homeSection);
  });
  const restoreButton = $("#restoreBriefingsBtn");
  if (restoreButton) restoreButton.hidden = dismissed.size === 0;
}

function dismissHomeSection(sectionId) {
  if (!sectionId) return;
  state.dismissedHomeSections = [...new Set([...state.dismissedHomeSections, sectionId])];
  localStorage.setItem("hira_pwa_dismissed_home_sections", JSON.stringify(state.dismissedHomeSections));
  applyHomeSectionDismissals();
  setStatus("Briefing closed.", "ok");
}

function restoreHomeSections() {
  state.dismissedHomeSections = [];
  localStorage.removeItem("hira_pwa_dismissed_home_sections");
  applyHomeSectionDismissals();
  setStatus("Briefings restored.", "ok");
}

function setView(name) {
  state.currentView = name;
  document.querySelectorAll(".nav-tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.view === name);
  });
  document.querySelectorAll(".view").forEach((view) => {
    view.classList.toggle("active", view.id === `${name}View`);
  });
}

function lessonDisplayName(lesson) {
  if (!lesson) return "";
  return [lesson.subject, lesson.class].filter(Boolean).join(" ") || lesson.title || "Lesson";
}

function rightNowCountdown(nextLesson) {
  if (!nextLesson || !Number.isFinite(Number(nextLesson.minutes_until))) return "";
  const elapsed = Math.floor(Math.max(0, Date.now() - Number(state.rightNowReceivedAt || Date.now())) / 60000);
  const remaining = Math.max(0, Number(nextLesson.minutes_until) - elapsed);
  return remaining <= 0 ? "starting now" : `${remaining}m`;
}

function renderRightNow(data = state.rightNow) {
  const strip = $("#rightNowStrip");
  if (!strip) return;
  const current = data?.current_lesson || null;
  const next = data?.next_lesson || null;
  const files = Array.isArray(data?.files) ? data.files.filter((item) => item?.url).slice(0, 4) : [];
  if (!current && !next && !files.length) {
    strip.hidden = true;
    return;
  }
  strip.hidden = false;
  const active = current || next;
  $("#rightNowTitle").textContent = current ? lessonDisplayName(current) : next ? `Next: ${lessonDisplayName(next)}` : "No lesson";
  $("#rightNowMeta").textContent = active
    ? [active.room, `${active.start || "--:--"}-${active.end || "--:--"}`, data?.week].filter(Boolean).join(" · ")
    : "School rhythm clear";
  $("#rightNowNextTitle").textContent = next ? lessonDisplayName(next) : "No next lesson";
  $("#rightNowNextMeta").textContent = next ? [rightNowCountdown(next), next.room].filter(Boolean).join(" · ") : "End-of-day";
  $("#rightNowFiles").innerHTML = files.length
    ? files.map((file) => `
        <a class="right-now-file" href="${escapeHtml(safeHttpUrl(file.url))}" target="_blank" rel="noopener noreferrer">
          <span data-lucide="file-text" aria-hidden="true"></span>
          ${escapeHtml(file.title || file.purpose_label || "ClassOps file")}
        </a>
      `).join("")
    : `<span class="right-now-file muted"><span data-lucide="folder-open" aria-hidden="true"></span>No linked files</span>`;
  refreshIcons(strip);
}

async function loadRightNow({ background = false, useCache = true } = {}) {
  if (state.rightNowRefreshInFlight) return state.rightNowRefreshInFlight;
  const snapshot = useCache ? readRightNowSnapshot() : null;
  if (snapshot && !state.rightNow) {
    state.rightNow = snapshot.data;
    state.rightNowSavedAt = snapshot.savedAt;
    state.rightNowReceivedAt = snapshot.savedAt;
    renderRightNow(snapshot.data);
  }
  state.rightNowRefreshInFlight = (async () => {
    const controller = new AbortController();
    const hardTimeout = window.setTimeout(() => controller.abort(), 10000);
    try {
      const data = await api("/api/lesson/now", { headers: headers(false), signal: controller.signal });
      state.rightNow = data;
      state.rightNowSavedAt = Date.now();
      state.rightNowReceivedAt = Date.now();
      saveRightNowSnapshot(data);
      renderRightNow(data);
    } catch (error) {
      if (!state.rightNow) renderRightNow(null);
      if (!background) setStatus(`Right Now unavailable: ${error.message}`, "warn");
    } finally {
      window.clearTimeout(hardTimeout);
      state.rightNowRefreshInFlight = null;
    }
  })();
  return state.rightNowRefreshInFlight;
}

function startRightNowPolling() {
  if (state.rightNowPoll) return;
  state.rightNowPoll = window.setInterval(() => {
    renderRightNow();
    if (!document.hidden && state.currentView === "home") loadRightNow({ background: true });
  }, RIGHT_NOW_REFRESH_MS);
}

function renderHomeData(data = {}, { fromCache = false, savedAt = 0 } = {}) {
  updateLiveClock();
  $("#homeLivingTimeline").innerHTML = renderLivingTimeline(data.agenda_structured || {}, data.prayers || {});
  refreshIcons($("#homeLivingTimeline"));
  $("#homeProactive").innerHTML = renderProactiveQueue(data.proactive || {});
  $("#homeDigest").innerHTML = renderMorningDigest(data.digest || {}, { limit: 3 });
  $("#homeIslamic").innerHTML = renderTextBlock(data.islamic || "Islamic rhythm unavailable right now.");
  const fileLines = countMeaningfulLines(data.files);
  $("#fileMemoryValue").textContent = String(fileLines);
  $("#fileMemoryLabel").textContent = fileLines ? "MEMORY ITEMS INDEXED" : "MEMORY STANDBY";
  $("#fileMemoryValueHome").textContent = String(fileLines);
  $("#fileMemoryLabelHome").textContent = fileLines ? "MEMORY ITEMS INDEXED" : "MEMORY STANDBY";
  renderSegmentsAll(".file-memory-segments", fileMemorySegments(data.files), 12, fileLines > 8 ? "success" : "accent");
  const services = data.services || {};
  const connectedCount = CONNECTIONS.filter(({ key }) => Boolean(services[key])).length;
  const warningCount = CONNECTIONS.filter(({ key }) => {
    const state = String(services?._details?.[key]?.state || "").toLowerCase();
    return state === "reconnect" || state === "attention";
  }).length;
  $("#homeServicesSummary").textContent = `${connectedCount}/${CONNECTIONS.length}`;
  $("#homeServicesLabel").textContent = warningCount ? "SERVICE NEEDS ATTENTION" : connectedCount ? "SERVICES CONNECTED" : "AWAITING CONNECTION";
  renderSegmentsAll(".services-segments", Math.round((connectedCount / CONNECTIONS.length) * 12), 12, warningCount ? "danger" : connectedCount ? "accent" : "muted");
  renderConnections(services);
  renderDailyLoad(data.daily_load || {});
  renderBriefingDelivery(data.briefing_delivery || {});
  renderIntelligenceStack(data.intelligence || {});
  renderClassOpsStatus(data.classops || {});
  renderTodayFocus(data);
  homeGlyphDataReady = true;
  if (glyphMode === "load" || glyphMode === "next") renderNothingGlyph(glyphMode);
  const proactiveTop = Array.isArray(data.proactive?.top) ? data.proactive.top : [];
  const lead = proactiveTop[0] || null;
  if (lead) {
    $("#homeFocusValue").textContent = lead.title || "Priority";
    $("#homeFocusLabel").textContent = `${String(lead.priority || "medium").toUpperCase()} PRIORITY · ${String(lead.score || 0)}`;
  }
  const marking = data.marking || {};
  const totalScripts = Number(marking.total_scripts || 0);
  const markedScripts = Number(marking.marked_scripts || 0);
  const unmarkedScripts = Number(marking.unmarked_scripts || 0);
  $("#markingMarkedValue").textContent = String(markedScripts);
  $("#markingUnmarkedValue").textContent = String(unmarkedScripts);
  $("#markingMarkedValueHome").textContent = String(markedScripts);
  $("#markingUnmarkedValueHome").textContent = String(unmarkedScripts);
  $("#markingStackCount").textContent = String(Number(marking.active_stacks || 0));
  $("#markingTotalValue").textContent = String(totalScripts);
  $("#markingSetsList").innerHTML = renderMarkingSets(marking.sets || []);
  $("#markingRailContext").textContent = markingRailContext(marking);
  renderSegmentsAll(".marked-segments", markingSegments(markedScripts, totalScripts), 12, "success");
  renderSegmentsAll(".unmarked-segments", markingSegments(unmarkedScripts, totalScripts), 12, unmarkedScripts > markedScripts ? "warning" : "accent");
  if (fromCache) setStatus(`Instant view from ${homeSnapshotAgeLabel(savedAt)}. Syncing quietly.`, "muted");
}

function renderHomeLoadingState() {
  $("#focusNowTitle").textContent = "Syncing";
  $("#focusNowMeta").textContent = "Checking today";
  $("#focusNextTitle").textContent = "Standby";
  $("#focusNextMeta").textContent = "No next item yet";
  $("#focusDueCount").textContent = "0";
  $("#focusDueMeta").textContent = "today";
  $("#focusMarkingCount").textContent = "0";
  $("#homeLivingTimeline").innerHTML = "<div>Loading...</div>";
  homeGlyphDataReady = false;
  $("#homeProactive").innerHTML = "<div>Loading...</div>";
  $("#homeDigest").innerHTML = "<div>Loading...</div>";
  $("#homeIslamic").innerHTML = "<div>Loading...</div>";
  $("#classOpsStatusList").innerHTML = "<div class=\"classops-status-row is-empty\"><span>ClassOps</span><strong>Checking status</strong><small>Loading submission ledger...</small></div>";
}

function renderHomeErrorState(error) {
  $("#homeLivingTimeline").textContent = `Error: ${error.message}`;
  $("#homeProactive").textContent = `Error: ${error.message}`;
  $("#homeDigest").textContent = `Error: ${error.message}`;
  $("#homeIslamic").textContent = `Error: ${error.message}`;
  $("#fileMemoryValue").textContent = "--";
  $("#fileMemoryLabel").textContent = "MEMORY CHECK FAILED";
  $("#fileMemoryValueHome").textContent = "--";
  $("#fileMemoryLabelHome").textContent = "MEMORY CHECK FAILED";
  renderBriefingDelivery({
    overall: "unknown",
    summary: "Digest delivery status unavailable.",
    slots: [],
  });
  renderSegmentsAll(".file-memory-segments", 1, 12, "warning");
}

function homeSlowSyncNote(timings = []) {
  const candidates = (Array.isArray(timings) ? timings : [])
    .filter((item) => item && item.phase !== "total" && (item.status !== "ok" || Number(item.elapsed_ms || 0) >= 3000))
    .sort((a, b) => Number(b.elapsed_ms || 0) - Number(a.elapsed_ms || 0));
  const slow = candidates[0];
  if (!slow) return "";
  const label = String(slow.phase || "source").replace(/^snapshot\./, "").replace(/^extra\./, "").replaceAll("_", " ");
  const seconds = Math.max(1, Math.round(Number(slow.elapsed_ms || 0) / 1000));
  return `${label} ${slow.status === "timeout" ? "timed out" : "was slow"} (${seconds}s)`;
}

async function loadHome({ force = false, background = false, useCache = true } = {}) {
  if (state.homeRefreshInFlight && !force) return state.homeRefreshInFlight;
  const refreshButton = $("#refreshHomeBtn");
  const snapshot = useCache ? readHomeSnapshot() : null;
  const canShowCache = Boolean(snapshot && !force);
  if (canShowCache) {
    renderHomeData(snapshot.data, { fromCache: true, savedAt: snapshot.savedAt });
  } else {
    renderHomeLoadingState();
  }
  if (refreshButton) {
    refreshButton.disabled = true;
    refreshButton.textContent = canShowCache || background ? "Syncing" : "Refreshing";
    refreshButton.classList.remove("is-updated");
  }
  state.homeLastRefreshStartedAt = Date.now();
  state.homeRefreshInFlight = (async () => {
    const controller = new AbortController();
    const slowNotice = window.setTimeout(() => {
      if (refreshButton) refreshButton.textContent = "Still Syncing";
      setStatus("Still syncing live sources. Cached view stays usable meanwhile.", "muted");
    }, 7000);
    const hardTimeout = window.setTimeout(() => controller.abort(), 28000);
    try {
      const data = await api(`/api/home?days=${state.homeDays}`, { headers: headers(false), signal: controller.signal });
      saveHomeSnapshot(data);
      renderHomeData(data);
      const slowNote = homeSlowSyncNote(data.sync_timings);
      setStatus(slowNote ? `Synced ${state.homeDays}-day view. Slow source: ${slowNote}.` : `Synced ${state.homeDays}-day view.`, slowNote ? "warn" : "ok");
      if (refreshButton) {
        refreshButton.textContent = "Updated";
        refreshButton.classList.add("is-updated");
        window.setTimeout(() => {
          refreshButton.innerHTML = `<span data-lucide="rotate-ccw" aria-hidden="true"></span>Refresh System`;
          refreshIcons(refreshButton);
          refreshButton.classList.remove("is-updated");
        }, 1400);
      }
    } catch (error) {
      if (!canShowCache) renderHomeErrorState(error);
      const message = error.name === "AbortError" ? "Live sync timed out after 28s." : error.message;
      setStatus(canShowCache ? `Live sync failed; cached view kept: ${message}` : message, canShowCache ? "warn" : "error");
      if (refreshButton) refreshButton.textContent = canShowCache ? "Retry Sync" : "Try again";
    } finally {
      window.clearTimeout(slowNotice);
      window.clearTimeout(hardTimeout);
      if (refreshButton) refreshButton.disabled = false;
      state.homeRefreshInFlight = null;
    }
  })();
  return state.homeRefreshInFlight;
}

async function checkForAppUpdate({ silent = false } = {}) {
  if (!("serviceWorker" in navigator)) return false;
  try {
    const registration = await navigator.serviceWorker.ready;
    await registration.update();
    if (registration.installing || registration.waiting) {
      setStatus("New app update found. Reloading...", "ok");
      registration.waiting?.postMessage({ type: "SKIP_WAITING" });
      return true;
    }
    if (!silent) setStatus("Dashboard refreshed. App shell is up to date.", "ok");
    await renderAppVersion();
    return false;
  } catch (error) {
    if (!silent) setStatus(`App update check: ${error.message}`, "warn");
    await renderAppVersion();
    return false;
  }
}

async function refreshHomeAndApp() {
  await loadHome({ force: true, useCache: false });
  await checkForAppUpdate();
}

function currentAgendaDays() {
  return Number($("#agendaDays")?.value || 7);
}

async function refreshAgendaSurfaces() {
  await loadHome({ background: true });
  await loadAgenda(currentAgendaDays());
}

function renderAgendaData(data = {}) {
  $("#agendaOutput").innerHTML = data.structured ? renderAgendaStructured(data.structured) : renderAgendaCards(data.text);
}

async function loadAgenda(days = 7, { force = false, useCache = true } = {}) {
  const snapshot = useCache ? readAgendaSnapshot(days) : null;
  if (snapshot && !force) {
    renderAgendaData(snapshot.data);
    setStatus(`Instant agenda from ${homeSnapshotAgeLabel(snapshot.savedAt)}. Syncing latest.`, "muted");
  } else {
    $("#agendaOutput").innerHTML = "<div>Loading...</div>";
  }
  try {
    const data = await api(`/api/agenda?days=${days}`, { headers: headers(false) });
    saveAgendaSnapshot(days, data);
    renderAgendaData(data);
    setStatus("Agenda refreshed.", "ok");
  } catch (error) {
    if (!snapshot || force) $("#agendaOutput").textContent = `Error: ${error.message}`;
    setStatus(snapshot && !force ? `Live agenda failed; cached agenda kept: ${error.message}` : error.message, snapshot && !force ? "warn" : "error");
  }
}

async function loadTasks(days = 30) {
  $("#tasksOutput").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/tasks?days=${days}`, { headers: headers(false) });
    $("#tasksOutput").innerHTML = data.structured ? renderTaskList(data.structured) : renderTaskBriefFromText(data.text);
    refreshIcons($("#tasksOutput"));
    setStatus("Tasks refreshed.", "ok");
  } catch (error) {
    $("#tasksOutput").textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

function renderMailList(messages) {
  const output = $("#gmailOutput");
  output.innerHTML = "";
  if (!messages.length) {
    output.innerHTML = "<div>No messages found.</div>";
    return;
  }
  for (const msg of messages) {
    const item = document.createElement("article");
    item.className = "mail";
    item.innerHTML = `
      <strong></strong>
      <small></small>
      <span></span>
      <button type="button" class="ghost-btn mail-action">Draft reply</button>
    `;
    item.querySelector("strong").textContent = msg.subject || "(No subject)";
    item.querySelector("small").textContent = `${msg.from || ""} | ${msg.date || ""}`;
    item.querySelector("span").textContent = msg.snippet || "";
    item.querySelector(".mail-action").addEventListener("click", () => {
      $("#draftAccount").value = $("#gmailAccount").value;
      $("#draftTo").value = "";
      $("#draftSubject").value = `Re: ${msg.subject || ""}`.trim();
      $("#draftBody").value = `Hi,\n\n\n\nBest,\nHerwanto`;
      setView("gmail");
      $("#draftSubject").scrollIntoView({ behavior: "smooth", block: "center" });
    });
    output.appendChild(item);
  }
}

async function loadGmail(event) {
  if (event) event.preventDefault();
  const output = $("#gmailOutput");
  output.innerHTML = "<div>Loading...</div>";
  try {
    const data = await api("/api/gmail", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({
        account: $("#gmailAccount").value,
        query: $("#gmailQuery").value.trim(),
        max_items: Number($("#gmailCount").value || 10),
      }),
    });
    renderMailList(data.messages || []);
    setStatus(`${(data.messages || []).length} emails loaded from ${data.account}.`, "ok");
  } catch (error) {
    output.textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

async function createDraft(event) {
  event.preventDefault();
  $("#draftStatus").textContent = "Creating draft...";
  try {
    const data = await api("/api/gmail/draft", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({
        account: $("#draftAccount").value,
        to: $("#draftTo").value.trim(),
        cc: $("#draftCc").value.trim(),
        subject: $("#draftSubject").value.trim(),
        body: $("#draftBody").value,
      }),
    });
    $("#draftStatus").textContent = `Draft created in ${data.account} Gmail.`;
    setStatus("Draft created.", "ok");
  } catch (error) {
    $("#draftStatus").textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

async function loadFilesLibrary() {
  if (!$("#fileLibrary")) return;
  $("#fileLibrary").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api("/api/files", { headers: headers(false) });
    $("#fileLibrary").innerHTML = renderTextBlock(data.text);
  } catch (error) {
    $("#fileLibrary").textContent = `Error: ${error.message}`;
  }
}

async function uploadFile(event) {
  event.preventDefault();
  const file = $("#fileInput").files[0];
  if (!file) return;
  $("#fileOutput").textContent = file.type.startsWith("audio/") ? "Transcribing..." : "Analysing...";
  const form = new FormData();
  form.append("file", file);
  form.append("note", $("#fileNote").value.trim());
  try {
    const data = await submitUploadJob(form, (job) => {
      $("#fileOutput").textContent = `Analysing ${file.name}... ${job.status}`;
    });
    $("#fileOutput").innerHTML = renderChatText(data.reply || "Done.");
    setStatus(`${file.name} analysed.`, "ok");
  } catch (error) {
    $("#fileOutput").textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

function chatAttachmentLabel(files = state.chatAttachments) {
  if (!files.length) return "";
  if (files.length === 1) return files[0].name;
  const names = files.slice(0, 2).map((file) => file.name).join(", ");
  const extra = files.length > 2 ? ` +${files.length - 2} more` : "";
  return `${files.length} files: ${names}${extra}`;
}

function setChatAttachments(files) {
  state.chatAttachments = Array.from(files || []).filter(Boolean);
  const chip = $("#chatAttachment");
  if (chip) {
    chip.hidden = !state.chatAttachments.length;
    $("#chatAttachmentName").textContent = chatAttachmentLabel();
    refreshIcons(chip);
  }
  updateComposerState();
}

function clearChatAttachment() {
  setChatAttachments([]);
  $("#chatFileInput").value = "";
}

function composerHasPayload() {
  return Boolean($("#messageInput")?.value.trim()) || state.chatAttachments.length > 0;
}

function updateComposerState() {
  const composer = $("#chatForm");
  const sendButton = $("#sendBtn");
  const voiceButton = $("#voiceRecordBtn");
  const hasPayload = composerHasPayload();
  if (composer) {
    composer.classList.toggle("has-input", hasPayload);
    composer.classList.toggle("is-busy", state.chatBusy);
    composer.classList.toggle("is-recording", state.voiceRecording);
  }
  if (sendButton) {
    sendButton.disabled = state.chatBusy || !hasPayload;
    sendButton.classList.toggle("is-ready", hasPayload && !state.chatBusy);
  }
  if (voiceButton) {
    voiceButton.disabled = state.chatBusy && !state.voiceRecording;
  }
}

function updateVoiceRecordButton() {
  const button = $("#voiceRecordBtn");
  if (!button) return;
  button.classList.toggle("is-recording", state.voiceRecording);
  button.title = state.voiceRecording ? "Stop recording" : "Tap to speak";
  button.setAttribute("aria-label", button.title);
  button.innerHTML = `<span data-lucide="${state.voiceRecording ? "square" : "mic"}" aria-hidden="true"></span>`;
  refreshIcons(button);
  updateComposerState();
}

function stopVoiceTracks() {
  state.voiceStream?.getTracks?.().forEach((track) => track.stop());
  state.voiceStream = null;
}

function voiceRecorderOptions() {
  if (!window.MediaRecorder) return null;
  const candidates = ["audio/webm;codecs=opus", "audio/webm", "audio/mp4"];
  const mimeType = candidates.find((type) => MediaRecorder.isTypeSupported?.(type));
  return mimeType ? { mimeType } : {};
}

async function startVoiceRecording() {
  if (!navigator.mediaDevices?.getUserMedia || !window.MediaRecorder) {
    setStatus("Voice capture is not available in this browser. Type instead.", "warn");
    return;
  }
  if (state.chatBusy) return;
  try {
    state.voiceStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    state.voiceChunks = [];
    const options = voiceRecorderOptions();
    state.voiceRecorder = new MediaRecorder(state.voiceStream, options || undefined);
    state.voiceRecorder.addEventListener("dataavailable", (event) => {
      if (event.data?.size) state.voiceChunks.push(event.data);
    });
    state.voiceRecorder.addEventListener("stop", () => {
      const type = state.voiceRecorder?.mimeType || "audio/webm";
      const blob = new Blob(state.voiceChunks, { type });
      state.voiceRecorder = null;
      state.voiceRecording = false;
      stopVoiceTracks();
      updateVoiceRecordButton();
      transcribeVoiceBlob(blob).catch((error) => {
        console.error(error);
        setStatus(error.message, "error");
      });
    }, { once: true });
    state.voiceRecorder.start();
    state.voiceRecording = true;
    updateVoiceRecordButton();
    setStatus("Listening. Tap mic again to stop.", "muted");
  } catch (error) {
    stopVoiceTracks();
    state.voiceRecording = false;
    updateVoiceRecordButton();
    const denied = error?.name === "NotAllowedError" || error?.name === "SecurityError";
    setStatus(denied ? "Mic permission denied. Type instead." : `Mic failed: ${error.message}`, "warn");
  }
}

function stopVoiceRecording() {
  if (state.voiceRecorder && state.voiceRecording) {
    state.voiceRecorder.stop();
  }
}

async function transcribeVoiceBlob(blob) {
  if (!blob?.size) {
    setStatus("No voice captured. Try once more.", "warn");
    return;
  }
  const form = new FormData();
  const extension = blob.type.includes("mp4") ? "m4a" : "webm";
  form.append("file", blob, `voice.${extension}`);
  setStatus("Transcribing voice...", "muted");
  const data = await api("/api/voice/transcribe", {
    method: "POST",
    headers: headers(false),
    body: form,
    retryNetwork: true,
    retryLabel: "voice transcription",
  });
  const text = String(data.text || "").trim();
  if (!text) {
    setStatus("I could not catch that. Type it instead.", "warn");
    return;
  }
  const input = $("#messageInput");
  if (input) {
    input.value = text;
    input.style.height = "auto";
    input.style.height = `${Math.min(input.scrollHeight, 180)}px`;
  }
  updateComposerState();
  if (!state.chatBusy) {
    if (input) {
      input.value = "";
      input.style.height = "auto";
    }
    updateComposerState();
    sendChat(text);
  }
}

function stageCommandPrompt(prompt) {
  const input = $("#messageInput");
  if (!input) return;
  input.value = prompt;
  input.style.height = "auto";
  input.style.height = `${Math.min(input.scrollHeight, 180)}px`;
  input.focus();
  input.setSelectionRange(input.value.length, input.value.length);
  updateComposerState();
  pulseComposerInput();
}

function runQuickCommand(button) {
  const prompt = button?.dataset.commandPrompt?.trim();
  const action = button?.dataset.commandAction || "fill";
  if (!prompt || state.chatBusy) return;
  closeQuickDrawer();
  hapticTap(10);
  if (action === "send") {
    $("#messageInput").value = "";
    updateComposerState();
    if (state.chatAttachments.length) {
      uploadChatAttachment(prompt);
    } else {
      sendChat(prompt);
    }
  } else {
    stageCommandPrompt(prompt);
  }
  setStatus(COMMAND_STATUS[action] || COMMAND_STATUS.fill, "ok");
}

function openQuickDrawer() {
  const drawer = $("#quickActionDrawer");
  if (!drawer) return;
  drawer.hidden = false;
  document.body.classList.add("quick-drawer-open");
  refreshIcons(drawer);
}

function closeQuickDrawer() {
  const drawer = $("#quickActionDrawer");
  if (!drawer) return;
  drawer.hidden = true;
  document.body.classList.remove("quick-drawer-open");
}

async function jumpToQuickView(view) {
  closeQuickDrawer();
  setView(view);
  if (view === "tasks") await loadTasks(Number($("#tasksDays")?.value || 30));
  if (view === "gmail") $("#gmailQuery")?.focus();
}

function taskDescriptionFromControl(control) {
  return control?.closest?.(".task-item")?.querySelector(".task-copy p")?.textContent?.trim() || "";
}

function planTaskFromControl(control) {
  const description = taskDescriptionFromControl(control);
  const prompt = description
    ? `Help me finish this task efficiently: ${description}. Give me the next 3 actions and the smallest first move.`
    : "Help me finish this selected task efficiently. Give me the next 3 actions and the smallest first move.";
  setView("home");
  sendChat(prompt);
}

function snoozeTaskFromControl(control) {
  const description = taskDescriptionFromControl(control);
  const prompt = description
    ? `Remind me tonight to handle this task: ${description}`
    : "Remind me tonight to handle this task.";
  setView("home");
  sendChat(prompt);
}

function pulseComposerInput() {
  const composer = $("#chatForm");
  if (!composer) return;
  composer.classList.remove("input-pulse");
  void composer.offsetWidth;
  composer.classList.add("input-pulse");
  const now = Date.now();
  if (now - state.lastInputPulseAt > 220) {
    hapticTap(6);
    state.lastInputPulseAt = now;
  }
}

function refreshAgendaSurfacesSoon() {
  refreshAgendaSurfaces().catch((error) => {
    setStatus(`Background refresh: ${error.message}`, "warn");
  });
}

async function uploadChatAttachment(note) {
  if (state.chatBusy || !state.chatAttachments.length) return;
  const files = [...state.chatAttachments];
  const fileLabel = chatAttachmentLabel(files);
  const userText = note
    ? `Attached ${fileLabel}\n\n${note}`
    : `Attached ${fileLabel}`;
  state.chatBusy = true;
  addMessage("user", userText);
  const pending = addMessage("hira", "", true);
  pending.classList.add("pending");
  setHiraSpeaking(pending, true);
  $("#attachBtn").disabled = true;
  updateComposerState();
  clearChatAttachment();
  try {
    setStatus(files.some((file) => file.type.startsWith("audio/")) ? "Transcribing attachments..." : "Analysing attachments...", "muted");
    const results = [];
    for (const [index, file] of files.entries()) {
      const form = new FormData();
      form.append("file", file);
      form.append("note", note || "Analyse this upload for reminders, follow-ups, deadlines, schedule items, and useful next actions.");
      const data = await submitUploadJob(form, (job) => {
        updateMessage(pending, `Analysing ${index + 1}/${files.length}: ${file.name}... ${job.status}`);
      });
      results.push({ file: file.name, reply: data.reply || "Done.", index: data.index || "" });
    }
    let reply = results[0]?.reply || "Done.";
    if (results.length > 1) {
      updateMessage(pending, "Combining attachment findings...");
      const combined = [
        "I uploaded several files in chat. Combine the findings into one concise answer. Preserve any concrete actions, dates, reminders, schedule items, score/classlist details, and unresolved questions.",
        note ? `User note: ${note}` : "",
        ...results.map((item, index) => `File ${index + 1}: ${item.file}\n${item.index ? `Index: ${item.index}\n` : ""}Analysis:\n${item.reply}`),
      ].filter(Boolean).join("\n\n");
      reply = await streamChatResponse(combined, (event, streamedText = "") => {
        if (event.type === "text" || event.type === "replace") {
          updateMessage(pending, visibleChatText(streamedText));
        }
      });
    }
    const visibleReply = visibleChatText(reply, { final: true });
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    updateMessage(pending, visibleReply);
    clearToolStatuses(pending);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: visibleReply };
    saveChatHistory();
    setStatus(`${files.length} attachment${files.length === 1 ? "" : "s"} analysed.`, "ok");
    maybeAutoSpeak(visibleReply);
  } catch (error) {
    const friendly = `I could not analyse the attachment${files.length === 1 ? "" : "s"}: ${error.message}`;
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    updateMessage(pending, friendly);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: friendly };
    saveChatHistory();
    console.error(error);
    setStatus(error.message, "error");
  } finally {
    state.chatBusy = false;
    $("#attachBtn").disabled = false;
    updateComposerState();
    refreshAgendaSurfacesSoon();
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function submitUploadJob(form, onProgress) {
  const requestId = window.crypto?.randomUUID?.() || `upload-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  form.append("request_id", requestId);
  const created = await api("/api/upload/jobs", {
    method: "POST",
    headers: headers(false),
    body: form,
    retryNetwork: true,
    retryLabel: "upload start",
  });
  let job = created;
  onProgress?.(job);
  for (let attempt = 0; attempt < 180; attempt += 1) {
    if (job.status === "done") return job;
    if (job.status === "error" || job.status === "missing") {
      throw new Error(job.error || "Upload analysis failed.");
    }
    await wait(attempt < 10 ? 1000 : 2000);
    job = await api(`/api/upload/jobs/${encodeURIComponent(job.job_id)}`, {
      headers: headers(false),
      retryNetwork: true,
      retryLabel: "upload status check",
    });
    onProgress?.(job);
  }
  throw new Error("Upload analysis is still running. Check Files again in a moment.");
}

async function sendChat(message) {
  if (state.chatBusy) return;
  state.chatBusy = true;
  updateComposerState();
  addMessage("user", message);
  const pending = addMessage("hira", "", true);
  pending.classList.add("pending");
  setHiraSpeaking(pending, true);
  let latestText = "";
  let activeToolName = "";
  let understanding = null;
  let trace = null;
  const stopProgress = startChatProgress(pending, () => latestText, () => activeToolName);
  try {
    const reply = await streamChatResponse(message, (event, streamedText = latestText) => {
      if (event.type === "route") {
        setStatus(event.name === "quick" ? "Quick reply path." : "Thinking with tools ready.", "muted");
      }
      if (event.type === "trace") {
        trace = event.trace || trace;
        renderTrace(pending, trace);
      }
      if (event.type === "understood") {
        understanding = {
          subject: event.subject || "",
          action: event.action || "",
          conflict: event.conflict || "",
        };
        renderUnderstanding(pending, understanding);
      }
      if (event.type === "tools") {
        console.info("H.I.R.A tool route", event.names || []);
      }
      if (event.type === "timing") {
        console.info(`H.I.R.A timing: ${event.phase}`, `${event.elapsed_ms}ms`, `phase ${event.phase_ms}ms`);
      }
      if (event.type === "continuation") {
        setStatus("Continuing response to avoid cutoff...", "muted");
      }
      if (event.type === "tool") {
        activeToolName = event.name || activeToolName;
        appendToolStatus(pending, event.name);
      }
      if (event.type === "notifications_archived" && Array.isArray(event.ids)) {
        const archivedIds = new Set(event.ids.map(String));
        state.dismissedNotificationIds.push(...archivedIds);
        saveDismissedNotificationIds();
        state.notifications = state.notifications.filter((item) => !archivedIds.has(String(item.id)));
        saveNotifications();
        renderNotifications();
      }
      if (event.type === "text" || event.type === "replace") {
        latestText = visibleChatText(streamedText);
        pending.classList.toggle("pending", !latestText);
        updateMessage(pending, latestText);
        renderUnderstanding(pending, understanding);
      }
    });
    const visibleReply = visibleChatText(reply, { final: true });
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    updateMessage(pending, visibleReply);
    renderUnderstanding(pending, understanding);
    renderTrace(pending, trace);
    clearToolStatuses(pending);
    state.chatHistory[state.chatHistory.length - 1] = CHAT_DEBUG_TRACE
      ? { role: "hira", text: visibleReply, trace }
      : { role: "hira", text: visibleReply };
    saveChatHistory();
    setStatus("H.I.R.A replied.", "ok");
    maybeAutoSpeak(visibleReply);
  } catch (error) {
    const friendly = "H.I.R.A hit a backend snag. Try again in a moment.";
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    clearToolStatuses(pending);
    updateMessage(pending, friendly);
    renderTrace(pending, trace);
    state.chatHistory[state.chatHistory.length - 1] = CHAT_DEBUG_TRACE
      ? { role: "hira", text: friendly, trace }
      : { role: "hira", text: friendly };
    saveChatHistory();
    console.error(error);
    setStatus(friendly, "error");
  } finally {
    stopProgress();
    state.chatBusy = false;
    updateComposerState();
    refreshAgendaSurfacesSoon();
  }
}

async function clearChat() {
  try {
    await api("/api/chat/reset", { method: "POST", headers: headers(false) });
  } catch (_) {
    // keep going; local reset is still useful
  }
  state.chatHistory = [];
  saveChatHistory();
  renderStoredChat();
  setStatus("Chat cleared.", "ok");
}

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.deferredInstall = event;
  $("#installBtn").hidden = false;
});

window.addEventListener("online", () => setStatus("Back online.", "ok"));
window.addEventListener("offline", () => setStatus("Offline. Cached app still works, live data will wait.", "warn"));

$("#installBtn").addEventListener("click", async () => {
  if (!state.deferredInstall) return;
  state.deferredInstall.prompt();
  await state.deferredInstall.userChoice;
  state.deferredInstall = null;
  $("#installBtn").hidden = true;
});

function setSettingsPanelOpen(open, { scroll = false } = {}) {
  const panel = $("#settingsPanel");
  panel.hidden = !open;
  $("#settingsBtn").classList.toggle("is-open", open);
  updateNotificationControls();
  if (open) {
    renderAppVersion();
    loadApiSpend({ quiet: true });
    loadActionLedger({ quiet: true });
    if (scroll) panel.scrollIntoView({ block: "start" });
  }
}

$("#settingsBtn").addEventListener("click", () => {
  setSettingsPanelOpen($("#settingsPanel").hidden);
});
$("#notificationsBtn").addEventListener("click", () => {
  const panel = $("#notificationsPanel");
  panel.hidden = !panel.hidden;
  $("#notificationsBtn").classList.toggle("is-open", !panel.hidden);
  renderNotifications();
  updateNotificationControls();
});
$("#notificationsList").addEventListener("click", (event) => {
  const actionButton = event.target.closest("[data-notification-action]");
  if (actionButton) {
    const id = actionButton.dataset.notificationId;
    const item = state.notifications.find((notification) => String(notification.id) === String(id)) || { id };
    performNotificationAction(actionButton.dataset.notificationAction, item);
    return;
  }
  const open = event.target.closest("[data-notification-open]");
  if (open) {
    const id = open.dataset.notificationOpen;
    const item = state.notifications.find((notification) => String(notification.id) === String(id)) || { id };
    openNotificationReader(item);
    return;
  }
  const feedback = event.target.closest("[data-feedback-rating]");
  if (feedback) {
    const id = feedback.dataset.feedbackTarget;
    const rating = feedback.dataset.feedbackRating;
    if (rating === "not_now") {
      const item = state.notifications.find((notification) => String(notification.id) === String(id)) || { id };
      performNotificationAction("not_now", item);
    } else {
      sendInsightFeedback(id, rating);
    }
    return;
  }
  const dismiss = event.target.closest("[data-notification-dismiss]");
  if (!dismiss) return;
  dismissNotification(dismiss.dataset.notificationDismiss);
});
$("#notificationReader").addEventListener("click", (event) => {
  const actionButton = event.target.closest("[data-notification-action]");
  if (actionButton) {
    const id = actionButton.dataset.notificationId;
    const item = state.activeNotificationItem || state.notifications.find((notification) => String(notification.id) === String(id)) || { id };
    performNotificationAction(actionButton.dataset.notificationAction, item);
    closeNotificationReader();
    return;
  }
  const close = event.target.closest("[data-reader-close]");
  if (close) {
    closeNotificationReader();
    return;
  }
  const dismiss = event.target.closest("#notificationReaderDismissBtn");
  if (dismiss) {
    dismissNotification(dismiss.dataset.notificationDismiss);
    closeNotificationReader();
  }
});
document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && !$("#notificationReader").hidden) closeNotificationReader();
  if (event.key === "Escape" && !$("#quickActionDrawer")?.hidden) closeQuickDrawer();
});
$("#enableNotificationsBtn").addEventListener("click", enableNotifications);
$("#settingsEnableNotificationsBtn").addEventListener("click", enableNotifications);
$("#testNotificationsBtn").addEventListener("click", sendTestNotification);
$("#checkHealthBtn").addEventListener("click", checkNotificationHealth);
$("#checkApiSpendBtn").addEventListener("click", () => loadApiSpend());
$("#refreshActionLedgerBtn").addEventListener("click", () => loadActionLedger());
$("#actionLedgerList").addEventListener("click", (event) => {
  const review = event.target.closest("[data-ledger-review]");
  if (review) {
    reviewActionEntry(review.dataset.ledgerReview);
    return;
  }
  const undo = event.target.closest("[data-ledger-undo]");
  if (undo && !undo.disabled) undoActionEntry(undo.dataset.ledgerUndo);
});
$("#checkAppUpdateBtn").addEventListener("click", async () => {
  const button = $("#checkAppUpdateBtn");
  const previousLabel = button.textContent;
  button.disabled = true;
  button.textContent = "Checking";
  try {
    await checkForAppUpdate({ silent: false });
    await renderAppVersion();
  } finally {
    button.disabled = false;
    button.textContent = previousLabel;
  }
});
document.querySelectorAll("[data-theme-choice], .theme-btn").forEach((button) => {
  button.dataset.theme = button.dataset.themeChoice || button.id.replace("theme", "").replace("Btn", "").toLowerCase();
  button.addEventListener("click", () => {
    state.theme = button.dataset.theme;
    localStorage.setItem("hira_theme", state.theme);
    applyTheme();
    setStatus(`Theme set to ${state.theme}.`, "ok");
  });
});
$("#autoSpeakToggle")?.addEventListener("change", (event) => {
  state.autoSpeak = Boolean(event.currentTarget.checked);
  localStorage.setItem(AUTO_SPEAK_KEY, state.autoSpeak ? "1" : "0");
  setStatus(state.autoSpeak ? "Auto-speak replies on." : "Auto-speak replies off.", "ok");
});
$("#saveTokenBtn").addEventListener("click", async () => {
  const button = $("#saveTokenBtn");
  const previous = button.textContent;
  button.disabled = true;
  button.textContent = "Unlocking";
  try {
    await createSession($("#tokenInput").value);
    $("#settingsPanel").hidden = true;
    setStatus("Session unlocked on this device.", "ok");
    updateNotificationControls();
    loadHome({ force: true, useCache: false });
    loadRightNow({ background: true, useCache: false });
    startRightNowPolling();
    startNotificationPolling();
  } catch (error) {
    $("#settingsPanel").hidden = false;
    setStatus(error.message, "error");
  } finally {
    button.disabled = false;
    button.textContent = previous;
  }
});
$("#clearTokenBtn").addEventListener("click", () => {
  state.token = "";
  state.sessionUnlocked = false;
  $("#tokenInput").value = "";
  localStorage.removeItem("hira_web_token");
  try {
    sessionStorage.removeItem(SESSION_TOKEN_KEY);
  } catch (_) {}
  localStorage.removeItem("hira_session_unlocked");
  state.rightNow = null;
  renderRightNow(null);
  fetch("/api/auth/logout", {
    method: "POST",
    headers: state.clientId ? { "X-Hira-Client": state.clientId } : {},
  }).catch(() => {});
  setStatus("Saved token removed and session cleared.", "ok");
});

document.querySelectorAll(".nav-tab").forEach((tab) => {
  tab.addEventListener("click", async () => {
    const view = tab.dataset.view;
    setView(view);
    if (view === "home" && Date.now() - state.homeLastRefreshStartedAt > HOME_REFRESH_THROTTLE_MS) {
      await loadHome({ background: true });
    }
    if (view === "home") loadRightNow({ background: true });
    if (view === "agenda") await loadAgenda(currentAgendaDays());
    if (view === "tasks") await loadTasks(Number($("#tasksDays")?.value || 30));
  });
});

document.addEventListener("click", (event) => {
  const speakMessage = event.target.closest("[data-speak-message]");
  if (speakMessage) {
    event.preventDefault();
    event.stopPropagation();
    playSpeech(speakMessage.dataset.speakText || speakMessage.closest(".message")?.textContent || "", speakMessage);
    return;
  }
  const speakTarget = event.target.closest("[data-speak-target]");
  if (speakTarget) {
    event.preventDefault();
    event.stopPropagation();
    const target = document.getElementById(speakTarget.dataset.speakTarget || "");
    playSpeech(target?.textContent || "", speakTarget);
    return;
  }
  const taskDone = event.target.closest("[data-task-done-button]");
  if (taskDone) {
    completeTask(taskDone.dataset.taskDoneButton, taskDone);
    return;
  }
  const taskSnooze = event.target.closest("[data-task-snooze]");
  if (taskSnooze) {
    snoozeTaskFromControl(taskSnooze);
    return;
  }
  const taskPlan = event.target.closest("[data-task-plan]");
  if (taskPlan) {
    planTaskFromControl(taskPlan);
    return;
  }
  const button = event.target.closest("[data-command-prompt]");
  if (!button) return;
  runQuickCommand(button);
});

$("#quickActionFab")?.addEventListener("click", openQuickDrawer);
$("#quickActionDrawer")?.addEventListener("click", (event) => {
  if (event.target.closest("[data-quick-close]")) {
    closeQuickDrawer();
    return;
  }
  const viewButton = event.target.closest("[data-quick-view]");
  if (viewButton) {
    jumpToQuickView(viewButton.dataset.quickView);
  }
});

$("#nothingGlyphBtn")?.addEventListener("click", cycleNothingGlyph);
$("#nothingGlyphBtn")?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    cycleNothingGlyph();
  }
});

document.querySelectorAll("[data-gmail-preset]").forEach((button) => {
  button.addEventListener("click", () => {
    $("#gmailQuery").value = button.dataset.gmailPreset;
    loadGmail();
  });
});

$("#chatForm").addEventListener("submit", (event) => {
  event.preventDefault();
  const input = $("#messageInput");
  const message = input.value.trim();
  if (!message && !state.chatAttachments.length) return;
  hapticTap(12);
  input.value = "";
  input.style.height = "auto";
  updateComposerState();
  if (state.chatAttachments.length) {
    uploadChatAttachment(message);
    return;
  }
  sendChat(message);
});

$("#voiceRecordBtn").addEventListener("click", () => {
  if (state.voiceRecording) {
    stopVoiceRecording();
  } else {
    startVoiceRecording();
  }
});

$("#attachBtn").addEventListener("click", () => {
  $("#chatFileInput").click();
});

$("#chatFileInput").addEventListener("change", (event) => {
  const files = Array.from(event.currentTarget.files || []);
  if (!files.length) return;
  setChatAttachments(files);
  hapticTap(10);
  setStatus(`${files.length} file${files.length === 1 ? "" : "s"} ready for chat analysis.`, "ok");
});

$("#clearAttachmentBtn").addEventListener("click", clearChatAttachment);

$("#messageInput").addEventListener("input", (event) => {
  const el = event.currentTarget;
  el.style.height = "auto";
  el.style.height = `${Math.min(el.scrollHeight, 180)}px`;
  updateComposerState();
  if (el.value.trim()) pulseComposerInput();
});

$("#resetChatBtn").addEventListener("click", clearChat);
$("#gmailForm").addEventListener("submit", loadGmail);
$("#draftForm").addEventListener("submit", createDraft);
$("#uploadForm").addEventListener("submit", uploadFile);
$("#refreshHomeBtn").addEventListener("click", refreshHomeAndApp);
$("#viewAgendaBtn").addEventListener("click", async () => {
  setView("agenda");
  await loadAgenda(currentAgendaDays());
});
$("#timelineAgendaBtn").addEventListener("click", async () => {
  setView("agenda");
  await loadAgenda(currentAgendaDays());
});
$("#homeSettingsBtn").addEventListener("click", () => {
  const panel = $("#settingsPanel");
  const shouldOpen = panel.hidden;
  setSettingsPanelOpen(shouldOpen, { scroll: shouldOpen });
});
$("#restoreBriefingsBtn").addEventListener("click", restoreHomeSections);
document.querySelectorAll("[data-home-dismiss]").forEach((button) => {
  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    dismissHomeSection(button.dataset.homeDismiss);
  });
});
$("#refreshAgendaBtn").addEventListener("click", () => loadAgenda(currentAgendaDays(), { force: true, useCache: false }));
$("#agendaDays").addEventListener("change", () => loadAgenda(currentAgendaDays()));
$("#refreshTasksBtn").addEventListener("click", () => loadTasks(Number($("#tasksDays").value || 30)));
$("#tasksDays").addEventListener("change", () => loadTasks(Number($("#tasksDays").value || 30)));
$("#refreshFilesBtn").addEventListener("click", () => setStatus("File upload is ready.", "ok"));
// Covers task checkboxes wherever task cards are rendered.
document.addEventListener("change", (event) => {
  const checkbox = event.target.closest("[data-task-done]");
  if (!checkbox || !checkbox.checked) return;
  completeTask(checkbox.dataset.taskDone, checkbox);
});

if ("serviceWorker" in navigator) {
  let refreshingForServiceWorker = false;
  navigator.serviceWorker.addEventListener("controllerchange", () => {
    reportClientModeToServiceWorker();
    renderAppVersion();
    if (refreshingForServiceWorker) return;
    refreshingForServiceWorker = true;
    window.location.reload();
  });
  navigator.serviceWorker
    .register("/service-worker.js", { updateViaCache: "none" })
    .then((registration) => {
      registration.update();
      reportClientModeToServiceWorker();
      updateNotificationControls();
      renderAppVersion();
    })
    .catch(updateNotificationControls);
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type === "hira-notification") {
      rememberPushedNotification(event.data.item || {});
      return;
    }
    if (event.data?.type === "hira-notification-open") {
      openNotificationReader(event.data.item || {});
      return;
    }
    if (event.data?.type === "hira-notification-action") {
      performNotificationAction(event.data.action, event.data.item || {});
    }
  });
  navigator.serviceWorker.ready.then(reportClientModeToServiceWorker).catch(() => {});
}

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) {
    reportClientModeToServiceWorker();
    updateNotificationControls();
    if (state.sessionUnlocked && state.currentView === "home") loadRightNow({ background: true });
  }
});

window.matchMedia("(prefers-color-scheme: dark)").addEventListener?.("change", () => {
  if (state.theme === "auto") applyTheme();
});

$("#tokenInput").value = state.token;
if ($("#autoSpeakToggle")) $("#autoSpeakToggle").checked = state.autoSpeak;
localStorage.setItem("hira_client_id", state.clientId);
applyTheme();
refreshIcons();
mountChatInHome();
applyHomeSectionDismissals();
renderStoredChat();
rememberNotificationFromUrl();
mirrorStoredNotificationsToChat();
renderNotifications();
updateNotificationControls();
updateComposerState();
updateVoiceRecordButton();
renderAppVersion();
setView("home");
updateLiveClock();
renderNothingGlyph("time");
initBatteryGlyph();
setInterval(updateLiveClock, 1000);

async function startAuthenticatedApp() {
  await migrateLegacyToken();
  if (!state.sessionUnlocked) {
    openTokenSettings("Save the H.I.R.A web token in this installed app once to sync live data.");
    return;
  }
  loadHome({ background: true });
  loadRightNow({ background: true });
  startRightNowPolling();
  startNotificationPolling();
}

startAuthenticatedApp();
