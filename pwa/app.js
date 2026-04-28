const state = {
  token: localStorage.getItem("hira_web_token") || "",
  theme: localStorage.getItem("hira_theme") || "light",
  clientId: localStorage.getItem("hira_client_id") || (crypto?.randomUUID ? crypto.randomUUID() : `hira-${Date.now()}`),
  deferredInstall: null,
  currentView: "chat",
  homeDays: 7,
  chatBusy: false,
  chatHistory: JSON.parse(localStorage.getItem("hira_pwa_chat") || "[]"),
};

const quickPrompts = [
  "What do I need to focus on today?",
  "Show my last 5 work emails",
  "What tasks are due this week?",
  "Summarise this week in plain English",
];

const $ = (selector) => document.querySelector(selector);
const urlTheme = new URLSearchParams(window.location.search).get("theme");

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

function setStatus(text, tone = "muted") {
  const el = $("#statusLine");
  el.textContent = text;
  el.dataset.tone = tone;
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

function renderStoredChat() {
  $("#messages").innerHTML = "";
  if (!state.chatHistory.length) {
    addMessage("hira", "I’m here. Same Hira, less Telegram noise.", false);
    return;
  }
  for (const item of state.chatHistory) addMessage(item.role, item.text, false);
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
    $("#greetingDate").textContent = data.greeting;
    $("#greetingTime").textContent = data.time_label;
    $("#homeClockTime").textContent = (data.time_label || "").replace(" SGT", "");
    $("#homeClockDate").textContent = data.greeting || "";
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
    $("#fileMemoryValue").textContent = "!";
    $("#fileMemoryLabel").textContent = "MEMORY CHECK FAILED";
    $("#fileMemoryValueHome").textContent = "!";
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
    $("#tasksOutput").innerHTML = renderTextBlock(data.text);
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

async function sendChat(message) {
  if (state.chatBusy) return;
  state.chatBusy = true;
  addMessage("user", message);
  const pending = addMessage("hira", "Thinking...");
  $("#sendBtn").disabled = true;
  try {
    const data = await api("/api/chat", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ message }),
    });
    pending.querySelector(".message-body").innerHTML = renderChatText(data.reply || "Done.");
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: data.reply || "Done." };
    saveChatHistory();
    setStatus("Hira replied.", "ok");
  } catch (error) {
    pending.querySelector(".message-body").textContent = `Error: ${error.message}`;
    state.chatHistory[state.chatHistory.length - 1] = { role: "hira", text: `Error: ${error.message}` };
    saveChatHistory();
    setStatus(error.message, "error");
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

$("#settingsBtn").addEventListener("click", () => $("#settingsPanel").toggleAttribute("hidden"));
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
    setView("chat");
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
  if (!message) return;
  input.value = "";
  input.style.height = "auto";
  sendChat(message);
});

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

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/service-worker.js");
}

window.matchMedia("(prefers-color-scheme: dark)").addEventListener?.("change", () => {
  if (state.theme === "auto") applyTheme();
});

$("#tokenInput").value = state.token;
localStorage.setItem("hira_client_id", state.clientId);
applyTheme();
quickPrompts.forEach((text) => {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "prompt-chip";
  button.dataset.prompt = text;
  button.textContent = text;
  button.addEventListener("click", () => {
    $("#messageInput").value = text;
    setView("chat");
    $("#messageInput").focus();
  });
  $("#promptRow").appendChild(button);
});

renderStoredChat();
setView("chat");
loadHome();
