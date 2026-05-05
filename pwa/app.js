const state = {
  token: localStorage.getItem("hira_web_token") || "",
  theme: localStorage.getItem("hira_theme") || "light",
  clientId: localStorage.getItem("hira_client_id") || (crypto?.randomUUID ? crypto.randomUUID() : `hira-${Date.now()}`),
  deferredInstall: null,
  currentView: "home",
  homeDays: 7,
  chatBusy: false,
  chatAttachments: [],
  chatHistory: JSON.parse(localStorage.getItem("hira_pwa_chat") || "[]"),
  notifications: JSON.parse(localStorage.getItem("hira_pwa_notifications") || "[]"),
  dismissedNotificationIds: JSON.parse(localStorage.getItem("hira_pwa_dismissed_notification_ids") || "[]"),
  chatNotificationIds: JSON.parse(localStorage.getItem("hira_pwa_chat_notification_ids") || "[]"),
  feedback: JSON.parse(localStorage.getItem("hira_pwa_feedback") || "{}"),
  deviceLocation: JSON.parse(localStorage.getItem("hira_pwa_device_location") || "null"),
  notificationPoll: null,
  lastPushSyncAt: Number(localStorage.getItem("hira_pwa_last_push_sync_at") || "0"),
  lastInputPulseAt: 0,
};

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
const CONNECTIONS = [
  { key: "calendar", label: "Calendar", icon: "calendar" },
  { key: "google", label: "Google", icon: "sparkles" },
  { key: "work_drive", label: "Work Google Drive", icon: "folder" },
  { key: "personal_gmail", label: "Personal Gmail", icon: "mail" },
];

function updateLiveClock() {
  const now = new Date();
  const time = clockFormatter.format(now).replace(/^24:/, "00:");
  const date = dateFormatter.format(now).replace(",", "").toUpperCase();
  $("#greetingTime").textContent = time;
  $("#greetingDate").textContent = date;
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
  const base = {};
  if (json) base["Content-Type"] = "application/json";
  if (state.token) base["X-Hira-Token"] = state.token;
  if (state.clientId) base["X-Hira-Client"] = state.clientId;
  return base;
}

function withAuth(options = {}) {
  const next = { ...options, headers: { ...(options.headers || {}) } };
  if (state.token) next.headers["X-Hira-Token"] = state.token;
  if (state.clientId) next.headers["X-Hira-Client"] = state.clientId;
  return next;
}

async function api(path, options = {}, tokenPrompted = false) {
  const response = await fetch(path, withAuth(options));
  if (response.status === 401) {
    if (tokenPrompted) {
      $("#settingsPanel").hidden = false;
      throw new Error("That token was rejected. Paste the H.I.R.A web token in Settings and save it once.");
    }
    const token = prompt("Enter H.I.R.A web token");
    if (token) {
      state.token = token.trim();
      localStorage.setItem("hira_web_token", state.token);
      $("#tokenInput").value = state.token;
      return api(path, options, true);
    }
    $("#settingsPanel").hidden = false;
    throw new Error("H.I.R.A needs the web token before live data can load.");
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
    if (tokenPrompted) {
      $("#settingsPanel").hidden = false;
      throw new Error("That token was rejected. Paste the H.I.R.A web token in Settings and save it once.");
    }
    const token = prompt("Enter H.I.R.A web token");
    if (token) {
      state.token = token.trim();
      localStorage.setItem("hira_web_token", state.token);
      $("#tokenInput").value = state.token;
      return fetchWithToken(path, options, true);
    }
    $("#settingsPanel").hidden = false;
    throw new Error("H.I.R.A needs the web token before live data can load.");
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

function plainNotificationText(text) {
  return (text || "")
    .replace(/[*_`]/g, "")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
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
      return `
        <article class="notification-item ${kind}" data-notification-id="${id}">
          <div class="notification-item-head">
            <strong>${markdownish(item.title || "H.I.R.A")}</strong>
            ${created ? `<small>${markdownish(created)}</small>` : ""}
          </div>
          <p>${markdownish(plainNotificationText(item.body || ""))}</p>
          <button type="button" class="ghost-btn notification-dismiss" data-notification-dismiss="${id}">
            <span data-lucide="x" aria-hidden="true"></span>
            Dismiss
          </button>
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
    if (state.token) {
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
  if (!state.token) return false;
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
  if (!state.token) return false;
  const now = Date.now();
  if (!force && now - state.lastPushSyncAt < 1000 * 60 * 30) return true;
  await api("/api/notifications/subscribe", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ subscription }),
  });
  state.lastPushSyncAt = now;
  localStorage.setItem("hira_pwa_last_push_sync_at", String(now));
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

function rememberNotificationFromUrl() {
  const params = new URLSearchParams(window.location.search);
  if (!params.has("notification_id")) return;
  rememberPushedNotification({
    id: params.get("notification_id"),
    kind: params.get("notification_kind") || "reminder",
    source: params.get("notification_source") || "",
    title: params.get("notification_title") || "H.I.R.A",
    body: params.get("notification_body") || "",
  });
  params.delete("notification_id");
  params.delete("notification_kind");
  params.delete("notification_source");
  params.delete("notification_title");
  params.delete("notification_body");
  const query = params.toString();
  const nextUrl = `${window.location.pathname}${query ? `?${query}` : ""}${window.location.hash}`;
  window.history.replaceState({}, "", nextUrl);
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

async function pollNotifications() {
  if (!state.token && $("#settingsPanel")?.hidden === false) return;
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
  el.hidden = false;
  el.innerHTML = `
    <div class="status-row"><span>Push keys</span><strong>${data.push_public_key && data.push_private_key ? "Ready" : "Missing"}</strong></div>
    <div class="status-row"><span>Subscriptions</span><strong>${data.subscription_count || 0}</strong></div>
    <div class="status-row"><span>This device</span><strong>${data.current_client_subscribed ? "Connected" : "Not connected"}</strong></div>
    <div class="status-row"><span>Stale subs</span><strong>${data.stale_subscription_count || 0}</strong></div>
    <div class="status-row"><span>Queued</span><strong>${data.queued_notification_count || 0}</strong></div>
    <p class="subtle">${markdownish(deliveryRows || "No recent push delivery attempts logged.")}</p>
    <p class="subtle">${markdownish(outcomeRows || "No notification feedback captured yet.")}</p>
    <p class="subtle">${markdownish(prayerRows || "Prayer status unavailable.")}</p>
  `;
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
  $("#homeConnectionsList").innerHTML = CONNECTIONS
    .map(
      ({ key, label, icon }) => {
        const connected = Boolean(services?.[key]);
        return `
        <div class="connection-card ${connected ? "is-on" : "is-off"}">
          <div class="connection-icon"><span data-lucide="${icon}" aria-hidden="true"></span></div>
          <div>
            <span>${label}</span>
            <strong>${connected ? "On" : "Off"}</strong>
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

function renderMorningDigest(data = {}) {
  const items = Array.isArray(data.items) ? data.items : [];
  if (!items.length) {
    return `<div class="empty-state compact">No digest items returned yet.</div>`;
  }
  return items.map((item, index) => {
    const meta = [item.label, item.source].filter(Boolean).join(" · ");
    const why = item.why ? `<p><strong>Why:</strong> ${markdownish(item.why)}</p>` : "";
    const title = markdownish(item.title || "Digest item");
    const link = item.url
      ? `<p><a href="${encodeURI(item.url)}" target="_blank" rel="noopener">Read source</a></p>`
      : "";
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
  const toneClass = loadToneClass(today.tone);
  $("#dailyLoadTitle").textContent = today.label || "Today";
  $("#dailyLoadBadge").textContent = today.load || "Pretty chill";
  $("#dailyLoadBadge").className = `load-badge ${toneClass}`;
  const score = Number(today.score ?? 0);
  const scorePct = Math.max(0, Math.min(100, score));
  $("#dailyLoadScore").closest(".daily-load-score").className = `daily-load-score score-${String(today.tone || "green").toLowerCase()}`;
  $("#dailyLoadScore").closest(".daily-load-score").style.setProperty("--score-arc", `${scorePct * 2.7}deg`);
  $("#dailyLoadScore").textContent = String(today.score ?? 0);
  $("#dailyLoadScoreLabel").textContent = `${String(today.tone || "green").toUpperCase()} DAY`;
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

function escapeHtml(text) {
  return (text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function notificationKindClass(kind) {
  const clean = String(kind || "notice").trim().toLowerCase();
  return ["notice", "briefing", "reminder", "update", "test"].includes(clean) ? clean : "notice";
}

function markdownish(text) {
  return escapeHtml(text || "")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/\*([^*]+)\*/g, "<strong>$1</strong>")
    .replace(/_([^_]+)_/g, "<em>$1</em>")
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*/g, "");
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

function renderAgendaCards(text) {
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
  return `<div class="agenda-list">${cards.join("")}</div>`;
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

function renderTaskList(data, heading = "Task Brief · Now to 7 May") {
  const items = data?.items || [];
  if (!items.length) return "<div class='empty-state'>No active tasks in that window.</div>";
  return `
    <div class="task-brief-card">
      <div class="task-brief-head">${markdownish(heading)}</div>
      <div class="task-list">
      ${items
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
              </div>
            </article>
          `;
        })
        .join("")}
      </div>
    </div>
  `;
}

function renderTaskBriefFromText(text) {
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
  return renderTaskList({ items });
}

async function completeTask(taskId, checkbox) {
  checkbox.disabled = true;
  const taskItem = checkbox.closest(".task-item");
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
      await loadHome();
      if ($("#tasksView")?.classList.contains("active") || state.currentView === "tasks") {
        await loadTasks(Number($("#tasksDays")?.value || 7));
      }
    }, 500);
  } catch (error) {
    checkbox.checked = false;
    checkbox.disabled = false;
    setStatus(error.message, "error");
  }
}

function addMessage(role, text, persist = true) {
  const el = document.createElement("article");
  el.className = `message ${role}`;
  el.innerHTML = `<div class="message-body">${renderChatText(text)}</div>`;
  $("#messages").appendChild(el);
  scrollMessagesToBottom();
  if (persist) {
    state.chatHistory.push({ role, text });
    state.chatHistory = state.chatHistory.slice(-30);
    saveChatHistory();
    updateChatChrome();
  }
  return el;
}

function setHiraSpeaking(el, speaking) {
  el?.classList.toggle("speaking", Boolean(speaking));
  const signal = $("#hiraSignal");
  signal?.classList.toggle("is-speaking", Boolean(speaking));
  const label = $("#hiraSignalState");
  if (label) label.textContent = "Live";
  // Glyph strip — cascade when HIRA is processing, breathe at idle
  document.getElementById("topbarGlyph")?.classList.toggle("is-active", Boolean(speaking));
}

function updateMessage(el, text) {
  el.querySelector(".message-body").innerHTML = renderChatText(text || "");
  scrollMessagesToBottom();
}

function appendToolStatus(el, name) {
  const labels = {
    create_calendar_event: "Adding to calendar...",
    delete_calendar_event_by_text: "Checking your calendar...",
    add_reminder: "Saving a reminder...",
    get_assistant_context: "Checking your day...",
    get_timetable: "Checking the timetable...",
    get_task_brief: "Checking tasks...",
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
  const status = document.createElement("div");
  status.className = "tool-status";
  status.innerHTML = `<span data-lucide="loader-2" aria-hidden="true"></span>${labels[name] || "Using a tool..."}`;
  el.appendChild(status);
  refreshIcons(status);
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
    onEvent({ type: "text", text: data.reply || "Done." });
    onEvent({ type: "done", text: data.reply || "Done." });
    return data.reply || "Done.";
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
      const event = JSON.parse(line.slice(6));
      if (event.type === "text") streamedText += event.text || "";
      if (event.type === "replace") streamedText = event.text || "";
      if (event.type === "done") finalText = event.text || streamedText;
      onEvent(event, streamedText);
      if (event.type === "error") {
        throw new Error(event.message || "H.I.R.A hit a backend snag. Try again in a moment.");
      }
    }
  }
  return finalText || streamedText || "Done.";
}

function renderStoredChat() {
  $("#messages").innerHTML = "";
  for (const item of state.chatHistory) addMessage(item.role, item.text, false);
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

function setView(name) {
  state.currentView = name;
  document.querySelectorAll(".nav-tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.view === name);
  });
  document.querySelectorAll(".view").forEach((view) => {
    view.classList.toggle("active", view.id === `${name}View`);
  });
}

async function loadHome() {
  const refreshButton = $("#refreshHomeBtn");
  if (refreshButton) {
    refreshButton.disabled = true;
    refreshButton.textContent = "Refreshing";
    refreshButton.classList.remove("is-updated");
  }
  $("#homeAgenda").innerHTML = "<div>Loading...</div>";
  $("#homeProactive").innerHTML = "<div>Loading...</div>";
  $("#homeTasks").innerHTML = "<div>Loading...</div>";
  $("#homeIslamic").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/home?days=${state.homeDays}`, { headers: headers(false) });
    updateLiveClock();
    $("#homeAgenda").innerHTML = renderAgendaCards(data.agenda);
    $("#homeProactive").innerHTML = renderProactiveQueue(data.proactive || {});
    $("#homeDigest").innerHTML = renderMorningDigest(data.digest || {});
    $("#homeTasks").innerHTML = renderTaskBriefFromText(data.tasks);
    $("#homeIslamic").innerHTML = renderTextBlock(data.islamic || "Islamic rhythm unavailable right now.");
    const fileLines = countMeaningfulLines(data.files);
    $("#fileMemoryValue").textContent = String(fileLines);
    $("#fileMemoryLabel").textContent = fileLines ? "MEMORY ITEMS INDEXED" : "MEMORY STANDBY";
    $("#fileMemoryValueHome").textContent = String(fileLines);
    $("#fileMemoryLabelHome").textContent = fileLines ? "MEMORY ITEMS INDEXED" : "MEMORY STANDBY";
    renderSegmentsAll(".file-memory-segments", fileMemorySegments(data.files), 12, fileLines > 8 ? "success" : "accent");
    const services = data.services || {};
    const connectedCount = CONNECTIONS.filter(({ key }) => Boolean(services[key])).length;
    $("#homeServicesSummary").textContent = `${connectedCount}/${CONNECTIONS.length}`;
    $("#homeServicesLabel").textContent = connectedCount ? "SERVICES CONNECTED" : "AWAITING CONNECTION";
    renderSegmentsAll(".services-segments", Math.round((connectedCount / CONNECTIONS.length) * 12), 12, connectedCount ? "accent" : "muted");
    renderConnections(services);
    renderDailyLoad(data.daily_load || {});
    const agendaCount = countMeaningfulLines(data.agenda);
    const taskCount = countMeaningfulLines(data.tasks);
    const proactiveTop = Array.isArray(data.proactive?.top) ? data.proactive.top : [];
    const lead = proactiveTop[0] || null;
    $("#homeAgendaCount").textContent = String(agendaCount);
    $("#homeTaskCount").textContent = String(taskCount);
    renderSegments("#homeAgendaBar", Math.min(12, Math.max(1, agendaCount)), 12, "accent");
    renderSegments("#homeTaskBar", Math.min(12, Math.max(1, taskCount)), 12, taskCount > 6 ? "warning" : "accent");
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
    setStatus(`Loaded ${state.homeDays}-day view.`, "ok");
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
    $("#homeAgenda").textContent = `Error: ${error.message}`;
    $("#homeProactive").textContent = `Error: ${error.message}`;
    $("#homeDigest").textContent = `Error: ${error.message}`;
    $("#homeTasks").textContent = `Error: ${error.message}`;
    $("#homeIslamic").textContent = `Error: ${error.message}`;
    $("#fileMemoryValue").textContent = "--";
    $("#fileMemoryLabel").textContent = "MEMORY CHECK FAILED";
    $("#fileMemoryValueHome").textContent = "--";
    $("#fileMemoryLabelHome").textContent = "MEMORY CHECK FAILED";
    renderSegmentsAll(".file-memory-segments", 1, 12, "warning");
    setStatus(error.message, "error");
    if (refreshButton) refreshButton.textContent = "Try again";
  } finally {
    if (refreshButton) refreshButton.disabled = false;
  }
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
    return false;
  } catch (error) {
    if (!silent) setStatus(`App update check: ${error.message}`, "warn");
    return false;
  }
}

async function refreshHomeAndApp() {
  await loadHome();
  await checkForAppUpdate();
}

function currentAgendaDays() {
  return Number($("#agendaDays")?.value || 7);
}

async function refreshAgendaSurfaces() {
  await loadHome();
  await loadAgenda(currentAgendaDays());
}

async function loadAgenda(days = 7) {
  $("#agendaOutput").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/agenda?days=${days}`, { headers: headers(false) });
    $("#agendaOutput").innerHTML = data.structured ? renderAgendaStructured(data.structured) : renderAgendaCards(data.text);
    setStatus("Agenda refreshed.", "ok");
  } catch (error) {
    $("#agendaOutput").textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

async function loadTasks(days = 7) {
  $("#tasksOutput").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/tasks?days=${days}`, { headers: headers(false) });
    $("#tasksOutput").innerHTML = data.structured ? renderTaskList(data.structured) : renderTaskBriefFromText(data.text);
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
  const hasPayload = composerHasPayload();
  if (composer) {
    composer.classList.toggle("has-input", hasPayload);
    composer.classList.toggle("is-busy", state.chatBusy);
  }
  if (sendButton) {
    sendButton.disabled = state.chatBusy || !hasPayload;
    sendButton.classList.toggle("is-ready", hasPayload && !state.chatBusy);
  }
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
          updateMessage(pending, streamedText);
        }
      });
    }
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    updateMessage(pending, reply);
    clearToolStatuses(pending);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: reply };
    saveChatHistory();
    setStatus(`${files.length} attachment${files.length === 1 ? "" : "s"} analysed.`, "ok");
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
  const created = await api("/api/upload/jobs", {
    method: "POST",
    headers: headers(false),
    body: form,
  });
  let job = created;
  onProgress?.(job);
  for (let attempt = 0; attempt < 180; attempt += 1) {
    if (job.status === "done") return job;
    if (job.status === "error" || job.status === "missing") {
      throw new Error(job.error || "Upload analysis failed.");
    }
    await wait(attempt < 10 ? 1000 : 2000);
    job = await api(`/api/upload/jobs/${encodeURIComponent(job.job_id)}`, { headers: headers(false) });
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
  try {
    let latestText = "";
    const reply = await streamChatResponse(message, (event, streamedText = latestText) => {
      if (event.type === "route") {
        setStatus(event.name === "quick" ? "Quick reply path." : "Thinking with tools ready.", "muted");
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
      if (event.type === "tool") appendToolStatus(pending, event.name);
      if (event.type === "text" || event.type === "replace") {
        latestText = streamedText;
        pending.classList.toggle("pending", !latestText);
        updateMessage(pending, latestText);
      }
    });
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    updateMessage(pending, reply);
    clearToolStatuses(pending);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: reply };
    saveChatHistory();
    setStatus("H.I.R.A replied.", "ok");
  } catch (error) {
    const friendly = "H.I.R.A hit a backend snag. Try again in a moment.";
    pending.classList.remove("pending");
    setHiraSpeaking(pending, false);
    clearToolStatuses(pending);
    updateMessage(pending, friendly);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: friendly };
    saveChatHistory();
    console.error(error);
    setStatus(friendly, "error");
  } finally {
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

$("#settingsBtn").addEventListener("click", () => {
  const panel = $("#settingsPanel");
  panel.toggleAttribute("hidden");
  $("#settingsBtn").classList.toggle("is-open", !panel.hidden);
  updateNotificationControls();
});
$("#notificationsBtn").addEventListener("click", () => {
  const panel = $("#notificationsPanel");
  panel.hidden = !panel.hidden;
  $("#notificationsBtn").classList.toggle("is-open", !panel.hidden);
  renderNotifications();
  updateNotificationControls();
});
$("#notificationsList").addEventListener("click", (event) => {
  const feedback = event.target.closest("[data-feedback-rating]");
  if (feedback) {
    sendInsightFeedback(feedback.dataset.feedbackTarget, feedback.dataset.feedbackRating);
    return;
  }
  const dismiss = event.target.closest("[data-notification-dismiss]");
  if (!dismiss) return;
  dismissNotification(dismiss.dataset.notificationDismiss);
});
$("#enableNotificationsBtn").addEventListener("click", enableNotifications);
$("#settingsEnableNotificationsBtn").addEventListener("click", enableNotifications);
$("#testNotificationsBtn").addEventListener("click", sendTestNotification);
$("#checkHealthBtn").addEventListener("click", checkNotificationHealth);
document.querySelectorAll("[data-theme-choice], .theme-btn").forEach((button) => {
  button.dataset.theme = button.dataset.themeChoice || button.id.replace("theme", "").replace("Btn", "").toLowerCase();
  button.addEventListener("click", () => {
    state.theme = button.dataset.theme;
    localStorage.setItem("hira_theme", state.theme);
    applyTheme();
    setStatus(`Theme set to ${state.theme}.`, "ok");
  });
});
$("#saveTokenBtn").addEventListener("click", () => {
  state.token = $("#tokenInput").value.trim();
  localStorage.setItem("hira_web_token", state.token);
  $("#settingsPanel").hidden = true;
  setStatus("Token saved on this device.", "ok");
  updateNotificationControls();
  startNotificationPolling();
});
$("#clearTokenBtn").addEventListener("click", () => {
  state.token = "";
  $("#tokenInput").value = "";
  localStorage.removeItem("hira_web_token");
  setStatus("Saved token removed.", "ok");
});

document.querySelectorAll(".nav-tab").forEach((tab) => {
  tab.addEventListener("click", async () => {
    const view = tab.dataset.view;
    setView(view);
    if (view === "home") await loadHome();
    if (view === "agenda") await loadAgenda(currentAgendaDays());
    if (view === "tasks") await loadTasks(7);
  });
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
$("#homeSettingsBtn").addEventListener("click", () => {
  const panel = $("#settingsPanel");
  panel.hidden = false;
  $("#settingsBtn").classList.add("is-open");
  updateNotificationControls();
  panel.scrollIntoView({ block: "start" });
});
$("#refreshAgendaBtn").addEventListener("click", () => loadAgenda(currentAgendaDays()));
$("#agendaDays").addEventListener("change", () => loadAgenda(currentAgendaDays()));
$("#refreshTasksBtn").addEventListener("click", () => loadTasks(Number($("#tasksDays").value || 7)));
$("#tasksDays").addEventListener("change", () => loadTasks(Number($("#tasksDays").value || 7)));
$("#refreshFilesBtn").addEventListener("click", () => setStatus("File upload is ready.", "ok"));
// Covers both #tasksOutput (Tasks tab) and #homeTasks (Home panel)
document.addEventListener("change", (event) => {
  const checkbox = event.target.closest("[data-task-done]");
  if (!checkbox || !checkbox.checked) return;
  completeTask(checkbox.dataset.taskDone, checkbox);
});

if ("serviceWorker" in navigator) {
  let refreshingForServiceWorker = false;
  navigator.serviceWorker.addEventListener("controllerchange", () => {
    if (refreshingForServiceWorker) return;
    refreshingForServiceWorker = true;
    window.location.reload();
  });
  navigator.serviceWorker
    .register("/service-worker.js", { updateViaCache: "none" })
    .then((registration) => {
      registration.update();
      updateNotificationControls();
    })
    .catch(updateNotificationControls);
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (event.data?.type !== "hira-notification") return;
    rememberPushedNotification(event.data.item || {});
  });
}

document.addEventListener("visibilitychange", () => {
  if (!document.hidden) updateNotificationControls();
});

window.matchMedia("(prefers-color-scheme: dark)").addEventListener?.("change", () => {
  if (state.theme === "auto") applyTheme();
});

$("#tokenInput").value = state.token;
localStorage.setItem("hira_client_id", state.clientId);
applyTheme();
refreshIcons();
mountChatInHome();
renderStoredChat();
rememberNotificationFromUrl();
mirrorStoredNotificationsToChat();
renderNotifications();
updateNotificationControls();
updateComposerState();
setView("home");
updateLiveClock();
setInterval(updateLiveClock, 1000);
loadHome();
startNotificationPolling();
