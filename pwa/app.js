const state = {
  token: localStorage.getItem("hira_web_token") || "",
  theme: localStorage.getItem("hira_theme") || "light",
  clientId: localStorage.getItem("hira_client_id") || (crypto?.randomUUID ? crypto.randomUUID() : `hira-${Date.now()}`),
  deferredInstall: null,
  currentView: "home",
  homeDays: 7,
  chatBusy: false,
  chatAttachment: null,
  chatHistory: JSON.parse(localStorage.getItem("hira_pwa_chat") || "[]"),
  notifications: JSON.parse(localStorage.getItem("hira_pwa_notifications") || "[]"),
  chatNotificationIds: JSON.parse(localStorage.getItem("hira_pwa_chat_notification_ids") || "[]"),
  notificationPoll: null,
};

const quickPrompts = [
  "What do I need to focus on today?",
  "Show my last 5 work emails",
  "What tasks are due this week?",
  "Summarise this week in plain English",
];

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

function updateLiveClock() {
  const now = new Date();
  const time = clockFormatter.format(now).replace(/^24:/, "00:");
  const date = dateFormatter.format(now).replace(",", "").toUpperCase();
  $("#greetingTime").textContent = time;
  $("#greetingDate").textContent = date;
  $("#homeClockTime").textContent = time;
  $("#homeClockDate").textContent = date;
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
  const themeColor = theme === "dark" ? "#000000" : "#f5f5f5";
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
      throw new Error("That token was rejected. Paste the Hira web token in Settings and save it once.");
    }
    const token = prompt("Enter Hira web token");
    if (token) {
      state.token = token.trim();
      localStorage.setItem("hira_web_token", state.token);
      $("#tokenInput").value = state.token;
      return api(path, options, true);
    }
    $("#settingsPanel").hidden = false;
    throw new Error("Hira needs the web token before live data can load.");
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
      throw new Error("That token was rejected. Paste the Hira web token in Settings and save it once.");
    }
    const token = prompt("Enter Hira web token");
    if (token) {
      state.token = token.trim();
      localStorage.setItem("hira_web_token", state.token);
      $("#tokenInput").value = state.token;
      return fetchWithToken(path, options, true);
    }
    $("#settingsPanel").hidden = false;
    throw new Error("Hira needs the web token before live data can load.");
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

function urlBase64ToUint8Array(value) {
  const padding = "=".repeat((4 - (value.length % 4)) % 4);
  const base64 = (value + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = atob(base64);
  return Uint8Array.from([...raw].map((char) => char.charCodeAt(0)));
}

function renderNotifications() {
  const badge = $("#notificationBadge");
  const list = $("#notificationsList");
  const panel = $("#notificationsPanel");
  const count = state.notifications.length;
  badge.hidden = count === 0;
  badge.textContent = String(Math.min(count, 99));
  if (!count) {
    list.innerHTML = "";
    if (panel) panel.hidden = true;
    return;
  }
  list.innerHTML = state.notifications
    .slice(0, 12)
    .map((item) => {
      const created = item.created ? new Date(item.created).toLocaleString([], { dateStyle: "medium", timeStyle: "short" }) : "";
      const id = escapeHtml(String(item.id || ""));
      return `
        <article class="notification-item ${item.kind || "notice"}" data-notification-id="${id}">
          <div class="notification-item-head">
            <strong>${markdownish(item.title || "Hira")}</strong>
            ${created ? `<small>${markdownish(created)}</small>` : ""}
          </div>
          <p>${markdownish(plainNotificationText(item.body || ""))}</p>
          <button type="button" class="ghost-btn notification-dismiss" data-notification-dismiss="${id}">
            <span data-lucide="x" aria-hidden="true"></span>
            Dismiss
          </button>
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
    const subscription = await registration.pushManager.getSubscription();
    setNotificationButtons({
      label: subscription ? "On" : "Setup",
      title: subscription
        ? "Browser permission and push subscription are active."
        : "Browser permission is on, but push still needs to be connected.",
      stateText: subscription
        ? "State: enabled and connected on this device."
        : "State: browser permission is on, push is not connected yet.",
      tone: subscription ? "ok" : "warn",
      disabled: !!subscription,
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
  if (!("serviceWorker" in navigator) || !("PushManager" in window)) return false;
  const config = await api("/api/notifications/config", { headers: headers(false) });
  const publicKey = (config.vapid_public_key || "").trim();
  if (!publicKey) return false;
  const registration = await navigator.serviceWorker.ready;
  const existing = await registration.pushManager.getSubscription();
  const subscription = existing || await registration.pushManager.subscribe({
    userVisibleOnly: true,
    applicationServerKey: urlBase64ToUint8Array(publicKey),
  });
  await api("/api/notifications/subscribe", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ subscription }),
  });
  return true;
}

async function showSystemNotification(item) {
  if (!("Notification" in window) || Notification.permission !== "granted") return;
  const title = item.title || "Hira";
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
  if (state.notifications.some((existing) => String(existing.id) === String(item.id))) {
    mirrorNotificationToChat(item);
    return false;
  }
  state.notifications.unshift(item);
  state.notifications = state.notifications.slice(0, 30);
  saveNotifications();
  renderNotifications();
  mirrorNotificationToChat(item);
  return true;
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
  const previous = state.notifications;
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
    state.notifications = previous;
    saveNotifications();
    renderNotifications();
    setStatus(`Could not dismiss notification: ${error.message}`, "warn");
  }
}

async function pollNotifications() {
  if (!state.token && $("#settingsPanel")?.hidden === false) return;
  try {
    const data = await api("/api/notifications?limit=12", { headers: headers(false) });
    const items = data.notifications || [];
    if (!items.length) return;
    const fresh = [];
    for (const item of items) {
      if (rememberNotification(item)) {
        fresh.push(item);
        showSystemNotification(item);
      }
    }
    await markNotificationsSeen(items.map((item) => item.id));
    if (fresh.length) setStatus(`${fresh.length} app notification${fresh.length === 1 ? "" : "s"} received.`, "ok");
  } catch (error) {
    if (!/token/i.test(error.message)) setStatus(`Notifications: ${error.message}`, "warn");
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

function renderConnections(services) {
  const labels = [
    ["Calendar", !!services.calendar],
    ["Google", !!services.google],
    ["Work Gmail", !!services.work_gmail],
    ["Personal Gmail", !!services.personal_gmail],
  ];
  $("#homeConnectionsList").innerHTML = labels
    .map(
      ([label, ok]) => `
        <div class="status-row">
          <span>${label}</span>
          <strong class="${ok ? "status-ok" : "status-off"}">${ok ? "CONNECTED" : "OFFLINE"}</strong>
        </div>
      `
    )
    .join("");
}

function fileMemorySegments(text) {
  const count = countMeaningfulLines(text);
  if (!count) return 1;
  return Math.max(2, Math.min(12, Math.ceil(count / 2)));
}

function markingSegments(value, total) {
  if (!total || total <= 0) return 0;
  return Math.max(1, Math.min(12, Math.round((value / total) * 12)));
}

function saveChatHistory() {
  localStorage.setItem("hira_pwa_chat", JSON.stringify(state.chatHistory.slice(-30)));
}

function saveChatNotificationIds() {
  state.chatNotificationIds = [...new Set(state.chatNotificationIds.map(String))].slice(-80);
  localStorage.setItem("hira_pwa_chat_notification_ids", JSON.stringify(state.chatNotificationIds));
}

function escapeHtml(text) {
  return (text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
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
    const title = line.replace(timeMatch?.[1] || "", "").replace(/^[:\-–\s]+/, "").trim() || line;
    cards.push(`
      <article class="agenda-card">
        <div class="agenda-time">${markdownish(time)}</div>
        <div class="agenda-copy">
          ${currentDay ? `<p class="agenda-day">${markdownish(currentDay)}</p>` : ""}
          <strong>${markdownish(title)}</strong>
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
                              <p class="agenda-day">${markdownish(item.label || item.kind || "Item")}</p>
                              <strong>${markdownish(item.title || "")}</strong>
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

function renderTaskList(data) {
  const items = data?.items || [];
  if (!items.length) return "<div class='empty-state'>No active tasks in that window.</div>";
  return `
    <div class="task-list">
      ${items
        .map((item) => {
          const due = [item.due, item.weekday].filter(Boolean).join(" | ");
          const meta = [item.category, item.priority, item.effort].filter(Boolean).join(" / ");
          return `
            <article class="task-item ${item.overdue ? "overdue" : ""}" data-task-id="${markdownish(item.id)}">
              <label class="task-check">
                <input type="checkbox" data-task-done="${markdownish(item.id)}" aria-label="Mark task ${markdownish(item.id)} done" />
                <span></span>
              </label>
              <div class="task-copy">
                <div class="task-meta">
                  <strong>#${markdownish(item.id)}</strong>
                  <span>${markdownish(due || "No due date")}</span>
                </div>
                <p>${markdownish(item.description || "")}</p>
                ${item.next_action ? `<small>Next: ${markdownish(item.next_action)}</small>` : ""}
                ${meta ? `<small>${markdownish(meta)}</small>` : ""}
              </div>
            </article>
          `;
        })
        .join("")}
    </div>
  `;
}

async function completeTask(taskId, checkbox) {
  checkbox.disabled = true;
  try {
    await api(`/api/tasks/${encodeURIComponent(taskId)}/done`, { method: "POST", headers: headers(false) });
    checkbox.closest(".task-item")?.classList.add("completed");
    setStatus(`Task #${taskId} completed.`, "ok");
    await loadHome();
    await loadTasks(Number($("#tasksDays").value || 7));
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
  el.scrollIntoView({ block: "end" });
  if (persist) {
    state.chatHistory.push({ role, text });
    state.chatHistory = state.chatHistory.slice(-30);
    saveChatHistory();
  }
  return el;
}

function updateMessage(el, text) {
  el.querySelector(".message-body").innerHTML = renderChatText(text || "");
  el.scrollIntoView({ block: "end" });
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
    get_latest_news: "Checking latest news...",
    web_search: "Searching...",
  };
  const status = document.createElement("div");
  status.className = "tool-status";
  status.innerHTML = `<span data-lucide="loader-2" aria-hidden="true"></span>${labels[name] || "Using a tool..."}`;
  el.appendChild(status);
  refreshIcons(status);
  el.scrollIntoView({ block: "end" });
}

async function streamChatResponse(message, onEvent) {
  const response = await fetchWithToken("/api/chat", {
    method: "POST",
    headers: headers(),
    body: JSON.stringify({ message }),
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
    }
  }
  return finalText || streamedText || "Done.";
}

function renderStoredChat() {
  $("#messages").innerHTML = "";
  if (!state.chatHistory.length) {
    addMessage("hira", "I’m here. Same Hira, less Telegram noise.", false);
    return;
  }
  for (const item of state.chatHistory) addMessage(item.role, item.text, false);
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
  $("#homeAgenda").innerHTML = "<div>Loading...</div>";
  $("#homeTasks").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/home?days=${state.homeDays}`, { headers: headers(false) });
    updateLiveClock();
    $("#homeAgenda").innerHTML = renderAgendaCards(data.agenda);
    $("#homeTasks").innerHTML = renderTextBlock(data.tasks);
    const fileLines = countMeaningfulLines(data.files);
    $("#fileMemoryValue").textContent = String(fileLines);
    $("#fileMemoryLabel").textContent = fileLines ? "MEMORY ITEMS INDEXED" : "MEMORY STANDBY";
    $("#fileMemoryValueHome").textContent = String(fileLines);
    $("#fileMemoryLabelHome").textContent = fileLines ? "MEMORY ITEMS INDEXED" : "MEMORY STANDBY";
    renderSegmentsAll(".file-memory-segments", fileMemorySegments(data.files), 12, fileLines > 8 ? "success" : "accent");
    const services = data.services || {};
    const connectedCount = Object.values(services).filter(Boolean).length;
    $("#homeServicesSummary").textContent = `${connectedCount}/4`;
    $("#homeServicesLabel").textContent = connectedCount ? "SERVICES CONNECTED" : "AWAITING CONNECTION";
    renderSegmentsAll(".services-segments", connectedCount * 3, 12, connectedCount ? "accent" : "muted");
    renderConnections(services);
    const agendaCount = countMeaningfulLines(data.agenda);
    const taskCount = countMeaningfulLines(data.tasks);
    $("#homeAgendaCount").textContent = String(agendaCount);
    $("#homeTaskCount").textContent = String(taskCount);
    renderSegments("#homeAgendaBar", Math.min(12, Math.max(1, agendaCount)), 12, "accent");
    renderSegments("#homeTaskBar", Math.min(12, Math.max(1, taskCount)), 12, taskCount > 6 ? "warning" : "accent");
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
    renderSegmentsAll(".marked-segments", markingSegments(markedScripts, totalScripts), 12, "success");
    renderSegmentsAll(".unmarked-segments", markingSegments(unmarkedScripts, totalScripts), 12, unmarkedScripts > markedScripts ? "warning" : "accent");
    setStatus(`Loaded ${state.homeDays}-day view.`, "ok");
  } catch (error) {
    $("#homeAgenda").textContent = `Error: ${error.message}`;
    $("#homeTasks").textContent = `Error: ${error.message}`;
    $("#fileMemoryValue").textContent = "--";
    $("#fileMemoryLabel").textContent = "MEMORY CHECK FAILED";
    $("#fileMemoryValueHome").textContent = "--";
    $("#fileMemoryLabelHome").textContent = "MEMORY CHECK FAILED";
    renderSegmentsAll(".file-memory-segments", 1, 12, "warning");
    setStatus(error.message, "error");
  }
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
    $("#tasksOutput").innerHTML = data.structured ? renderTaskList(data.structured) : renderTextBlock(data.text);
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
    const data = await api("/api/upload", {
      method: "POST",
      headers: headers(false),
      body: form,
    });
    $("#fileOutput").innerHTML = renderChatText(data.reply || "Done.");
    setStatus(`${file.name} analysed.`, "ok");
  } catch (error) {
    $("#fileOutput").textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

function setChatAttachment(file) {
  state.chatAttachment = file || null;
  const chip = $("#chatAttachment");
  if (!chip) return;
  chip.hidden = !state.chatAttachment;
  $("#chatAttachmentName").textContent = state.chatAttachment ? state.chatAttachment.name : "";
  refreshIcons(chip);
}

function clearChatAttachment() {
  setChatAttachment(null);
  $("#chatFileInput").value = "";
}

async function uploadChatAttachment(note) {
  if (state.chatBusy || !state.chatAttachment) return;
  const file = state.chatAttachment;
  const userText = note
    ? `Attached ${file.name}\n\n${note}`
    : `Attached ${file.name}`;
  state.chatBusy = true;
  addMessage("user", userText);
  const pending = addMessage("hira", "", true);
  pending.classList.add("pending");
  $("#sendBtn").disabled = true;
  $("#attachBtn").disabled = true;
  clearChatAttachment();
  const form = new FormData();
  form.append("file", file);
  form.append("note", note || "Analyse this upload for reminders, follow-ups, deadlines, schedule items, and useful next actions.");
  try {
    setStatus(file.type.startsWith("audio/") ? "Transcribing attachment..." : "Analysing attachment...", "muted");
    const data = await api("/api/upload", {
      method: "POST",
      headers: headers(false),
      body: form,
    });
    const reply = data.reply || "Done.";
    pending.classList.remove("pending");
    updateMessage(pending, reply);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: reply };
    saveChatHistory();
    await loadHome();
    setStatus(`${file.name} analysed.`, "ok");
  } catch (error) {
    const friendly = `I could not analyse ${file.name}: ${error.message}`;
    pending.classList.remove("pending");
    updateMessage(pending, friendly);
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: friendly };
    saveChatHistory();
    console.error(error);
    setStatus(error.message, "error");
  } finally {
    state.chatBusy = false;
    $("#sendBtn").disabled = false;
    $("#attachBtn").disabled = false;
  }
}

async function sendChat(message) {
  if (state.chatBusy) return;
  state.chatBusy = true;
  addMessage("user", message);
  const pending = addMessage("hira", "", true);
  pending.classList.add("pending");
  $("#sendBtn").disabled = true;
  try {
    let latestText = "";
    const reply = await streamChatResponse(message, (event, streamedText = latestText) => {
      if (event.type === "route") {
        setStatus(event.name === "quick" ? "Quick reply path." : "Thinking with tools ready.", "muted");
      }
      if (event.type === "tools") {
        console.info("Hira tool route", event.names || []);
      }
      if (event.type === "timing") {
        console.info(`Hira timing: ${event.phase}`, `${event.elapsed_ms}ms`, `phase ${event.phase_ms}ms`);
      }
      if (event.type === "tool") appendToolStatus(pending, event.name);
      if (event.type === "text" || event.type === "replace") {
        latestText = streamedText;
        pending.classList.toggle("pending", !latestText);
        updateMessage(pending, latestText);
      }
      if (event.type === "error") throw new Error(event.message);
    });
    pending.classList.remove("pending");
    updateMessage(pending, reply);
    pending.querySelectorAll(".tool-status").forEach((item) => item.remove());
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: reply };
    saveChatHistory();
    setStatus("Hira replied.", "ok");
  } catch (error) {
    const friendly = "Hira hit a backend snag. Try again in a moment.";
    pending.classList.remove("pending");
    pending.querySelector(".message-body").textContent = friendly;
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: friendly };
    saveChatHistory();
    console.error(error);
    setStatus(friendly, "error");
  } finally {
    state.chatBusy = false;
    $("#sendBtn").disabled = false;
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
  $("#settingsPanel").toggleAttribute("hidden");
  updateNotificationControls();
});
$("#notificationsBtn").addEventListener("click", () => {
  renderNotifications();
  if (state.notifications.length) $("#notificationsPanel").toggleAttribute("hidden");
  updateNotificationControls();
});
$("#notificationsList").addEventListener("click", (event) => {
  const dismiss = event.target.closest("[data-notification-dismiss]");
  if (!dismiss) return;
  dismissNotification(dismiss.dataset.notificationDismiss);
});
$("#enableNotificationsBtn").addEventListener("click", enableNotifications);
$("#settingsEnableNotificationsBtn").addEventListener("click", enableNotifications);
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
    if (view === "agenda") await loadAgenda(7);
    if (view === "tasks") await loadTasks(7);
  });
});

document.querySelectorAll("[data-home-days]").forEach((button) => {
  button.addEventListener("click", async () => {
    state.homeDays = Number(button.dataset.homeDays);
    document.querySelectorAll("[data-home-days]").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    await loadHome();
  });
});

document.querySelectorAll(".prompt-chip").forEach((button) => {
  button.addEventListener("click", () => {
    $("#messageInput").value = button.dataset.prompt;
    setView("home");
    $("#messageInput").focus();
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
  if (!message && !state.chatAttachment) return;
  input.value = "";
  input.style.height = "auto";
  if (state.chatAttachment) {
    uploadChatAttachment(message);
    return;
  }
  sendChat(message);
});

$("#attachBtn").addEventListener("click", () => {
  $("#chatFileInput").click();
});

$("#chatFileInput").addEventListener("change", (event) => {
  const file = event.currentTarget.files[0];
  if (!file) return;
  setChatAttachment(file);
  setStatus(`${file.name} ready for chat analysis.`, "ok");
});

$("#clearAttachmentBtn").addEventListener("click", clearChatAttachment);

$("#messageInput").addEventListener("input", (event) => {
  const el = event.currentTarget;
  el.style.height = "auto";
  el.style.height = `${Math.min(el.scrollHeight, 150)}px`;
});

$("#resetChatBtn").addEventListener("click", clearChat);
$("#gmailForm").addEventListener("submit", loadGmail);
$("#draftForm").addEventListener("submit", createDraft);
$("#uploadForm").addEventListener("submit", uploadFile);
$("#refreshHomeBtn").addEventListener("click", loadHome);
$("#refreshAgendaBtn").addEventListener("click", () => loadAgenda(Number($("#agendaDays").value || 7)));
$("#agendaDays").addEventListener("change", () => loadAgenda(Number($("#agendaDays").value || 7)));
$("#refreshTasksBtn").addEventListener("click", () => loadTasks(Number($("#tasksDays").value || 7)));
$("#tasksDays").addEventListener("change", () => loadTasks(Number($("#tasksDays").value || 7)));
$("#refreshFilesBtn").addEventListener("click", () => setStatus("File upload is ready.", "ok"));
$("#tasksOutput").addEventListener("change", (event) => {
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
quickPrompts.forEach((text) => {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "prompt-chip";
  button.dataset.prompt = text;
  button.textContent = text;
  button.addEventListener("click", () => {
    $("#messageInput").value = text;
    setView("home");
    $("#messageInput").focus();
  });
  $("#promptRow").appendChild(button);
  document.querySelector(".prompt-row-mirror")?.appendChild(button.cloneNode(true));
});

document.querySelectorAll(".prompt-row-mirror .prompt-chip").forEach((button) => {
  button.addEventListener("click", () => {
    $("#messageInput").value = button.dataset.prompt;
    $("#messageInput").focus();
  });
});

mountChatInHome();
renderStoredChat();
mirrorStoredNotificationsToChat();
renderNotifications();
updateNotificationControls();
setView("home");
updateLiveClock();
setInterval(updateLiveClock, 1000);
loadHome();
startNotificationPolling();
