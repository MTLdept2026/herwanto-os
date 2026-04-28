const state = {
  token: localStorage.getItem("hira_web_token") || "",
  deferredInstall: null,
};

const $ = (selector) => document.querySelector(selector);

function headers(json = true) {
  const base = {};
  if (json) base["Content-Type"] = "application/json";
  if (state.token) base["X-Hira-Token"] = state.token;
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

function setView(name) {
  document.querySelectorAll(".tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.view === name);
  });
  document.querySelectorAll(".view").forEach((view) => {
    view.classList.toggle("active", view.id === name);
  });
}

function addMessage(role, text) {
  const el = document.createElement("div");
  el.className = `message ${role}`;
  el.textContent = text;
  $("#messages").appendChild(el);
  el.scrollIntoView({ block: "end" });
}

async function sendChat(message) {
  addMessage("user", message);
  addMessage("hira", "Thinking...");
  const pending = $("#messages").lastElementChild;
  try {
    const data = await api("/api/chat", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({ message }),
    });
    pending.textContent = data.reply || "Done.";
  } catch (error) {
    pending.textContent = `Error: ${error.message}`;
  }
}

async function loadAgenda() {
  $("#agendaOutput").textContent = "Loading...";
  try {
    const data = await api("/api/agenda?days=7", { headers: headers(false) });
    $("#agendaOutput").textContent = data.text;
  } catch (error) {
    $("#agendaOutput").textContent = `Error: ${error.message}`;
  }
}

async function loadTasks() {
  $("#tasksOutput").textContent = "Loading...";
  try {
    const data = await api("/api/tasks?days=7", { headers: headers(false) });
    $("#tasksOutput").textContent = data.text;
  } catch (error) {
    $("#tasksOutput").textContent = `Error: ${error.message}`;
  }
}

async function loadGmail(event) {
  event.preventDefault();
  const output = $("#gmailOutput");
  output.textContent = "Loading...";
  try {
    const data = await api("/api/gmail", {
      method: "POST",
      headers: headers(),
      body: JSON.stringify({
        account: $("#gmailAccount").value,
        query: $("#gmailQuery").value.trim(),
        max_items: 10,
      }),
    });
    output.textContent = "";
    if (!data.messages.length) {
      output.textContent = "No messages found.";
      return;
    }
    for (const msg of data.messages) {
      const item = document.createElement("div");
      item.className = "mail";
      item.innerHTML = `<strong></strong><small></small><span></span>`;
      item.querySelector("strong").textContent = msg.subject || "(No subject)";
      item.querySelector("small").textContent = `From: ${msg.from || ""}`;
      item.querySelector("span").textContent = msg.snippet || "";
      output.appendChild(item);
    }
  } catch (error) {
    output.textContent = `Error: ${error.message}`;
  }
}

async function uploadFile(event) {
  event.preventDefault();
  const file = $("#fileInput").files[0];
  if (!file) return;
  $("#fileOutput").textContent = "Analysing...";
  const form = new FormData();
  form.append("file", file);
  form.append("note", $("#fileNote").value.trim());
  try {
    const data = await api("/api/upload", {
      method: "POST",
      headers: headers(false),
      body: form,
    });
    $("#fileOutput").textContent = data.reply || "Done.";
  } catch (error) {
    $("#fileOutput").textContent = `Error: ${error.message}`;
  }
}

window.addEventListener("beforeinstallprompt", (event) => {
  event.preventDefault();
  state.deferredInstall = event;
  $("#installBtn").hidden = false;
});

$("#installBtn").addEventListener("click", async () => {
  if (!state.deferredInstall) return;
  state.deferredInstall.prompt();
  await state.deferredInstall.userChoice;
  state.deferredInstall = null;
  $("#installBtn").hidden = true;
});

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => setView(tab.dataset.view));
});

$("#chatForm").addEventListener("submit", (event) => {
  event.preventDefault();
  const input = $("#messageInput");
  const message = input.value.trim();
  if (!message) return;
  input.value = "";
  sendChat(message);
});

$("#messageInput").addEventListener("input", (event) => {
  const el = event.currentTarget;
  el.style.height = "auto";
  el.style.height = `${Math.min(el.scrollHeight, 130)}px`;
});

$("[data-action='load-agenda']").addEventListener("click", loadAgenda);
$("[data-action='load-tasks']").addEventListener("click", loadTasks);
$("#gmailForm").addEventListener("submit", loadGmail);
$("#uploadForm").addEventListener("submit", uploadFile);

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.register("/service-worker.js");
}

addMessage("hira", "I’m here. Same Hira, less Telegram noise.");
