const state = {
  token: localStorage.getItem("hira_web_token") || "",
  data: null,
  selectedClass: "",
  selectedFolder: "",
  nonSubmitted: new Set(),
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

function shortDateTime(value = "") {
  if (!value) return "--";
  try {
    return new Date(value).toLocaleString("en-SG", {
      day: "2-digit",
      month: "short",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  } catch (_) {
    return String(value);
  }
}

function segmentMarkup(value, total = 12, tone = "accent") {
  const filled = Math.max(0, Math.min(total, Math.round(Number(value || 0))));
  return Array.from({ length: total }, (_, index) => `<span class="${index < filled ? `active ${tone}` : ""}"></span>`).join("");
}

function renderMissionTelemetry(data = {}) {
  const summary = data.summary || {};
  const students = data.student_summary || {};
  const concernCount = Number(students.concern_count || 0);
  const dueWork = Number(summary.collection_candidate_count || 0);
  const classes = Number(summary.class_count ?? data.class_count ?? 0);
  const roster = Number(students.roster_count || 0);
  const readiness = concernCount ? "Action" : classes ? "Nominal" : "Standby";
  $("#missionReadiness").textContent = readiness;
  $("#missionReadiness").dataset.tone = concernCount ? "warn" : classes ? "ok" : "muted";
  $("#missionScanTime").textContent = shortDateTime(data.generated_at);
  $("#missionFollowUp").textContent = String(concernCount);
  $("#missionReadout").innerHTML = `
    <p>${classes} classes online · ${roster} students synced from Drive.</p>
    <p>${dueWork} due-work signals · ${Number(students.assignment_count || 0)} tracked submissions.</p>
  `;
}

function renderSummary(data) {
  const summary = data?.summary || {};
  $("#classCount").textContent = String(summary.class_count ?? data?.class_count ?? "--");
  $("#lessonCount").textContent = String(summary.lesson_count ?? "--");
  $("#fileCount").textContent = String(summary.file_count ?? data?.file_count ?? "--");
  $("#collectCount").textContent = String(summary.collection_candidate_count ?? "--");
  $("#studentCount").textContent = String(data?.student_summary?.roster_count ?? "--");
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
            <div><span>Watch</span><strong>${Number(item.student_report?.concern_count || item.collection_candidate_count || 0)}</strong></div>
          </div>
          <div class="class-telemetry" aria-hidden="true">${segmentMarkup(Math.min(12, Number(item.lesson_count || 0)), 12, item.student_report?.concern_count ? "warn" : "accent")}</div>
          <button type="button" data-select-class="${escapeHtml(item.class)}">Open</button>
        </article>
      `).join("")
    : `<div class="empty">No class folders detected yet.</div>`;
}

function renderClassList(classes = []) {
  $("#classList").innerHTML = classes.map((item) => `
    <button type="button" class="${item.class === state.selectedClass ? "active" : ""}" data-select-class="${escapeHtml(item.class)}">
      <span>${escapeHtml(item.class)}</span>
      <strong>${Number(item.student_report?.concern_count || item.collection_candidate_count || 0)}</strong>
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
          <p>Detected as due work from the filename.</p>
        </article>
      `).join("")
    : `<div class="empty">No due-work signals detected from filenames yet.</div>`;
}

function renderContents(classItem) {
  if (!classItem) {
    $("#detailEyebrow").textContent = "Select class";
    $("#detailTitle").textContent = "Contents";
    $("#collectionPanel").innerHTML = "";
    $("#ledgerMeta").textContent = "--";
    $("#studentReport").innerHTML = `<div class="empty">Choose a class to load its roster.</div>`;
    $("#contentsTable").innerHTML = `<div class="empty">Choose a class to view its content page.</div>`;
    return;
  }
  $("#detailEyebrow").textContent = `${classItem.file_count || 0} files`;
  $("#detailTitle").textContent = `${classItem.class} Contents`;
  renderCollectionPanel(classItem);
  renderStudentReport(classItem.student_report || {});
  renderNonSubmissionRoster(classItem);
  const lessons = (classItem.folders || []).filter((folder) => folder.date || folder.folder !== ".");
  $("#contentsTable").innerHTML = `
    <div class="lesson-row header">
      <div>Date</div><div>Topic</div><div>Materials</div><div>Submission</div>
    </div>
    ${lessons.length ? lessons.map((folder) => {
      const dueWork = (folder.collection_candidates || []).map((file) => file.name).join(", ");
      return `
        <article class="lesson-row">
          <div><strong>${escapeHtml(folder.date || "Undated")}</strong><p class="folder-meta">${escapeHtml(folder.folder || "")}</p></div>
          <div>${escapeHtml(folder.topic || "-")}</div>
          <div>
            <ul class="file-list">
              ${(folder.files || []).slice(0, 12).map((file) => `<li>${escapeHtml(file.name)} <span>${escapeHtml(file.kind || "")}</span></li>`).join("")}
            </ul>
          </div>
          <div>
            <p class="due-work-name">${escapeHtml(dueWork || "No due work marked")}</p>
            <button type="button" data-track-lesson="${escapeHtml(folder.date || "")}" data-track-topic="${escapeHtml(folder.topic || "")}" data-track-folder="${escapeHtml(folder.folder || "")}">Submissions</button>
          </div>
        </article>
      `;
    }).join("") : `<div class="empty">No dated lesson folders found for this class.</div>`}
  `;
}

function renderStudentReport(report = {}) {
  const students = report.students || [];
  const concerns = report.concerns || [];
  $("#ledgerMeta").textContent = `${report.roster_count || 0} students · ${report.assignment_count || 0} tracked`;
  if (!students.length) {
    $("#studentReport").innerHTML = `<div class="empty">No roster returned yet. Check that the Google classlist sheets are shared with H.I.R.A.</div>`;
    return;
  }
  const visible = concerns.length ? concerns : students.slice(0, 10);
  const latest = (report.assignments || []).slice(-3).reverse();
  const health = report.roster_count ? Math.max(0, Math.min(12, 12 - Number(report.concern_count || 0))) : 0;
  $("#studentReport").innerHTML = `
    <div class="student-summary">
      <article><span>Roster</span><strong>${Number(report.roster_count || 0)}</strong></article>
      <article><span>Tracked work</span><strong>${Number(report.assignment_count || 0)}</strong></article>
      <article><span>Follow up</span><strong>${Number(report.concern_count || 0)}</strong></article>
    </div>
    <div class="submission-gauge" aria-label="Submission health">
      <div>${segmentMarkup(health, 12, report.concern_count ? "warn" : "success")}</div>
      <strong>${report.concern_count ? "Intervention path active" : "Submission channel clear"}</strong>
    </div>
    ${latest.length ? `
      <div class="assignment-tally">
        ${latest.map((assignment) => `
          <article>
            <strong>${escapeHtml(assignment.assignment_title || "Tracked work")}</strong>
            <span>${Number(assignment.submitted_count || 0)}/${Number(assignment.roster_count || 0)} submitted · ${Number(assignment.missing_count || 0)} pending</span>
          </article>
        `).join("")}
      </div>
    ` : ""}
    <div class="student-table">
      <div class="student-row header">
        <div>Name</div><div>Done</div><div>Missing</div><div>Absent</div><div>Catch-up</div><div>Status</div>
      </div>
      ${visible.map((student) => `
        <article class="student-row" data-status="${escapeHtml(student.status || "clear")}">
          <strong>${escapeHtml(student.name)}</strong>
          <div>${Number(student.submitted_count || 0)}</div>
          <div>${Number(student.missing_count || 0)}</div>
          <div>${Number(student.absent_count || 0)}</div>
          <div>${Number(student.catchup_count || 0)}</div>
          <div>${escapeHtml(student.status || "clear")}</div>
        </article>
      `).join("")}
    </div>
  `;
}

function currentClassItem() {
  return (state.data?.classes || []).find((item) => item.class === state.selectedClass);
}

function renderNonSubmissionRoster(classItem = currentClassItem()) {
  const students = classItem?.student_report?.students || classItem?.students || [];
  $("#nonSubmissionCount").textContent = `${state.nonSubmitted.size} selected`;
  if (!students.length) {
    $("#nonSubmissionRoster").innerHTML = `<div class="empty">No roster returned yet. Scan after Google Drive classlists are configured.</div>`;
    return;
  }
  $("#nonSubmissionRoster").innerHTML = students.map((student) => {
    const name = student.name || "";
    const checked = state.nonSubmitted.has(name) ? "checked" : "";
    return `
      <label class="student-chip">
        <input type="checkbox" value="${escapeHtml(name)}" ${checked} />
        <span>${escapeHtml(student.no ? `${student.no}. ` : "")}${escapeHtml(name)}</span>
      </label>
    `;
  }).join("");
}

function prefillLesson(button) {
  const date = button.dataset.trackLesson || "";
  state.selectedFolder = button.dataset.trackFolder || "";
  state.nonSubmitted = new Set();
  $("#lessonDateInput").value = /^\d{4}-\d{2}-\d{2}$/.test(date) ? date : "";
  $("#topicInput").value = button.dataset.trackTopic || "";
  const dueWork = (button.closest(".lesson-row")?.querySelector(".due-work-name")?.textContent || "").trim();
  if (dueWork && dueWork !== "No due work marked") $("#assignmentTitleInput").value = dueWork.split(",")[0].trim();
  renderNonSubmissionRoster();
  $("#assignmentTitleInput").focus();
}

function selectClass(className) {
  state.selectedClass = className;
  state.selectedFolder = "";
  state.nonSubmitted = new Set();
  const classItem = (state.data?.classes || []).find((item) => item.class === className);
  renderClassList(state.data?.classes || []);
  renderContents(classItem);
}

function renderDashboard(data) {
  state.data = data;
  const classes = data.classes || [];
  if (!state.selectedClass && classes.length) state.selectedClass = classes[0].class;
  renderSummary(data);
  renderMissionTelemetry(data);
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

$("#assignmentForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.selectedClass) return setStatus("Select a class first.", "warn");
  const payload = {
    class_name: state.selectedClass,
    lesson_date: $("#lessonDateInput").value,
    topic: $("#topicInput").value.trim(),
    folder: state.selectedFolder,
    assignment_title: $("#assignmentTitleInput").value.trim(),
    collect_by: $("#collectByInput").value,
    absent: [],
    submitted: [],
    non_submitted: [...state.nonSubmitted],
  };
  if (!payload.assignment_title) return setStatus("Add an assignment title before saving.", "warn");
  $("#saveAssignmentBtn").disabled = true;
  setStatus(`Saving ${state.selectedClass} tracking...`);
  try {
    const result = await api("/api/classops/assignment", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const classItem = (state.data?.classes || []).find((item) => item.class === state.selectedClass);
    if (classItem) classItem.student_report = result.report;
    renderStudentReport(result.report || {});
    state.nonSubmitted = new Set();
    renderNonSubmissionRoster(classItem);
    setStatus("Tracking saved.", "ok");
  } catch (error) {
    setStatus(error.message, "error");
  } finally {
    $("#saveAssignmentBtn").disabled = false;
  }
});

$("#clearNonSubmissionBtn").addEventListener("click", () => {
  state.nonSubmitted = new Set();
  renderNonSubmissionRoster();
});

$("#nonSubmissionRoster").addEventListener("change", (event) => {
  const input = event.target.closest("input[type='checkbox']");
  if (!input) return;
  if (input.checked) {
    state.nonSubmitted.add(input.value);
  } else {
    state.nonSubmitted.delete(input.value);
  }
  $("#nonSubmissionCount").textContent = `${state.nonSubmitted.size} selected`;
});

document.addEventListener("click", (event) => {
  const lessonButton = event.target.closest("[data-track-lesson]");
  if (lessonButton) {
    prefillLesson(lessonButton);
    return;
  }
  const button = event.target.closest("[data-select-class]");
  if (!button) return;
  selectClass(button.dataset.selectClass);
});

if (state.token) {
  $("#tokenPanel").hidden = true;
  loadDashboard();
}
