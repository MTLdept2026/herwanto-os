const state = {
  token: localStorage.getItem("hira_web_token") || "",
  theme: localStorage.getItem("hira_theme") || "light",
  clientId: localStorage.getItem("hira_client_id") || (crypto?.randomUUID ? crypto.randomUUID() : `hira-${Date.now()}`),
  deferredInstall: null,
  currentView: "home",
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
  document.querySelectorAll(".theme-btn").forEach((btn) => {
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

async function api(path, options = {}) {
  const response = await fetch(path, options);
  if (response.status === 401) {
    const token = prompt("Enter Hira web token");
    if (token) {
      state.token = token.trim();
      localStorage.setItem("hira_web_token", state.token);
      return api(path, options);
    }
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

function markingSegments(value, total) {
  if (!total || total <= 0) return 0;
  return Math.max(1, Math.min(12, Math.round((value / total) * 12)));
}

function saveChatHistory() {
  localStorage.setItem("hira_pwa_chat", JSON.stringify(state.chatHistory.slice(-30)));
}

function markdownish(text) {
  return (text || "")
    .replace(/\*([^*]+)\*/g, "<strong>$1</strong>")
    .replace(/_([^_]+)_/g, "<em>$1</em>")
    .replace(/`([^`]+)`/g, "<code>$1</code>");
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

function addMessage(role, text, persist = true) {
  const el = document.createElement("article");
  el.className = `message ${role}`;
  el.innerHTML = `<div class="message-body">${renderTextBlock(text)}</div>`;
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
  $("#homeFiles").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/home?days=${state.homeDays}`, { headers: headers(false) });
    $("#greetingDate").textContent = data.greeting;
    $("#greetingTime").textContent = data.time_label;
    $("#homeClockTime").textContent = (data.time_label || "").replace(" SGT", "");
    $("#homeClockDate").textContent = data.greeting || "";
    $("#homeAgenda").innerHTML = renderTextBlock(data.agenda);
    $("#homeTasks").innerHTML = renderTextBlock(data.tasks);
    $("#homeFiles").innerHTML = renderTextBlock(data.files);
    const services = data.services || {};
    const connectedCount = Object.values(services).filter(Boolean).length;
    $("#homeServicesSummary").textContent = `${connectedCount}/4`;
    $("#homeServicesLabel").textContent = connectedCount ? "SERVICES CONNECTED" : "AWAITING CONNECTION";
    renderSegments("#homeServicesBar", connectedCount * 3, 12, connectedCount ? "accent" : "muted");
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
    $("#markingStackCount").textContent = String(Number(marking.active_stacks || 0));
    $("#markingTotalValue").textContent = String(totalScripts);
    renderSegments("#markingMarkedBar", markingSegments(markedScripts, totalScripts), 12, "success");
    renderSegments("#markingUnmarkedBar", markingSegments(unmarkedScripts, totalScripts), 12, unmarkedScripts > markedScripts ? "warning" : "accent");
    setStatus(`Loaded ${state.homeDays}-day view.`, "ok");
  } catch (error) {
    $("#homeAgenda").textContent = `Error: ${error.message}`;
    $("#homeTasks").textContent = `Error: ${error.message}`;
    $("#homeFiles").textContent = `Error: ${error.message}`;
    setStatus(error.message, "error");
  }
}

async function loadAgenda(days = 7) {
  $("#agendaOutput").innerHTML = "<div>Loading...</div>";
  try {
    const data = await api(`/api/agenda?days=${days}`, { headers: headers(false) });
    $("#agendaOutput").innerHTML = renderTextBlock(data.text);
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
    $("#fileOutput").innerHTML = renderTextBlock(data.reply || "Done.");
    await loadFilesLibrary();
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
    pending.querySelector(".message-body").innerHTML = renderTextBlock(data.reply || "Done.");
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
document.querySelectorAll(".theme-btn").forEach((button) => {
  button.dataset.theme = button.id.replace("theme", "").replace("Btn", "").toLowerCase();
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
    if (view === "files") await loadFilesLibrary();
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
$("#refreshFilesBtn").addEventListener("click", loadFilesLibrary);

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
setView("home");
loadHome();
