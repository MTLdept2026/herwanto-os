const state = {
  token: localStorage.getItem("hira_web_token") || "",
  data: null,
  selectedClass: "",
};

const $ = (selector) => document.querySelector(selector);

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setStatus(message, tone = "muted") {
  const el = $("#statusText");
  el.textContent = message;
  el.dataset.tone = tone;
}

async function api(path, options = {}) {
  const headers = { ...(options.headers || {}) };
  if (state.token) headers["X-Hira-Token"] = state.token;
  const response = await fetch(path, { ...options, headers });
  const contentType = response.headers.get("content-type") || "";
  const data = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(typeof data === "string" ? data : data.detail || "Request failed");
  }
  return data;
}

function fileSummary(files = []) {
  if (!files.length) return "No files";
  return files
    .slice(0, 5)
    .map((file) => `${file.name}${file.kind ? ` (${file.kind})` : ""}`)
    .join(", ") + (files.length > 5 ? ` +${files.length - 5} more` : "");
}

function renderSummary(data) {
  const summary = data?.summary || {};
  $("#classCount").textContent = String(summary.class_count ?? data?.class_count ?? "--");
  $("#lessonCount").textContent = String(summary.lesson_count ?? "--");
  $("#fileCount").textContent = String(summary.file_count ?? data?.file_count ?? "--");
  $("#collectCount").textContent = String(summary.collection_candidate_count ?? "--");
}

function renderClassCards(classes = []) {
  $("#classGrid").innerHTML = classes.length
    ? classes.map((item) => `
        <article class="class-card">
          <p class="eyebrow">${escapeHtml(item.latest_lesson?.date || "No dated lesson")}</p>
          <h3>${escapeHtml(item.class)}</h3>
          <p>${escapeHtml(item.latest_lesson?.topic || item.latest_lesson?.folder || "Waiting for lesson folders.")}</p>
          <div class="metric-row">
            <div><span>Lessons</span><strong>${Number(item.lesson_count || 0)}</strong></div>
            <div><span>Files</span><strong>${Number(item.file_count || 0)}</strong></div>
            <div><span>Collect</span><strong>${Number(item.collection_candidate_count || 0)}</strong></div>
          </div>
          <button type="button" data-select-class="${escapeHtml(item.class)}">Open</button>
        </article>
      `).join("")
    : `<div class="empty">No class folders detected yet.</div>`;
}

function renderClassList(classes = []) {
  $("#classList").innerHTML = classes.map((item) => `
    <button type="button" class="${item.class === state.selectedClass ? "active" : ""}" data-select-class="${escapeHtml(item.class)}">
      <span>${escapeHtml(item.class)}</span>
      <strong>${Number(item.collection_candidate_count || 0)}</strong>
    </button>
  `).join("");
}

function renderCollectionPanel(classItem) {
  const candidates = classItem?.collection_candidates || [];
  $("#collectionPanel").innerHTML = candidates.length
    ? candidates.map((file) => `
        <article class="collection-item">
          <strong>${escapeHtml(file.name)}</strong>
          <small>${escapeHtml(file.path || "")}</small>
          <p>${escapeHtml(file.collection?.hint || "Likely collection item")}</p>
        </article>
      `).join("")
    : `<div class="empty">No likely collection items detected from filenames yet.</div>`;
}

function renderContents(classItem) {
  if (!classItem) {
    $("#detailEyebrow").textContent = "Select class";
    $("#detailTitle").textContent = "Contents";
    $("#collectionPanel").innerHTML = "";
    $("#contentsTable").innerHTML = `<div class="empty">Choose a class to view its content page.</div>`;
    return;
  }
  $("#detailEyebrow").textContent = `${classItem.file_count || 0} files`;
  $("#detailTitle").textContent = `${classItem.class} Contents`;
  renderCollectionPanel(classItem);
  const lessons = (classItem.folders || []).filter((folder) => folder.date || folder.folder !== ".");
  $("#contentsTable").innerHTML = `
    <div class="lesson-row header">
      <div>Date</div><div>Topic</div><div>Materials</div><div>Collection</div>
    </div>
    ${lessons.length ? lessons.map((folder) => {
      const collect = (folder.collection_candidates || []).map((file) => file.name).join(", ");
      return `
        <article class="lesson-row">
          <div><strong>${escapeHtml(folder.date || "Undated")}</strong><p class="folder-meta">${escapeHtml(folder.folder || "")}</p></div>
          <div>${escapeHtml(folder.topic || "-")}</div>
          <div>
            <ul class="file-list">
              ${(folder.files || []).slice(0, 12).map((file) => `<li>${escapeHtml(file.name)} <span>${escapeHtml(file.kind || "")}</span></li>`).join("")}
            </ul>
          </div>
          <div>${escapeHtml(collect || "-")}</div>
        </article>
      `;
    }).join("") : `<div class="empty">No dated lesson folders found for this class.</div>`}
  `;
}

function selectClass(className) {
  state.selectedClass = className;
  const classItem = (state.data?.classes || []).find((item) => item.class === className);
  renderClassList(state.data?.classes || []);
  renderContents(classItem);
}

function renderDashboard(data) {
  state.data = data;
  const classes = data.classes || [];
  if (!state.selectedClass && classes.length) state.selectedClass = classes[0].class;
  renderSummary(data);
  renderClassCards(classes);
  renderClassList(classes);
  renderContents(classes.find((item) => item.class === state.selectedClass));
  setStatus(`Last scan: ${data.generated_at || "unknown"}. Root: ${data.root || "/"}`, "ok");
}

async function loadDashboard() {
  if (!state.token) {
    setStatus("Save your H.I.R.A web token first.", "warn");
    return;
  }
  $("#scanBtn").disabled = true;
  setStatus("Scanning Dropbox ClassOps folder...");
  try {
    const data = await api("/api/classops/dashboard");
    renderDashboard(data);
  } catch (error) {
    setStatus(error.message, "error");
  } finally {
    $("#scanBtn").disabled = false;
  }
}

$("#saveTokenBtn").addEventListener("click", () => {
  state.token = $("#tokenInput").value.trim();
  localStorage.setItem("hira_web_token", state.token);
  setStatus("Token saved. Ready to scan.", "ok");
});

$("#scanBtn").addEventListener("click", loadDashboard);
$("#printBtn").addEventListener("click", () => window.print());

document.addEventListener("click", (event) => {
  const button = event.target.closest("[data-select-class]");
  if (!button) return;
  selectClass(button.dataset.selectClass);
});

if (state.token) {
  $("#tokenPanel").hidden = true;
  loadDashboard();
}
