const state = {
  token: localStorage.getItem("hira_web_token") || "",
  data: null,
  selectedClass: "",
  selectedFolder: "",
  selectedContentItem: null,
  selectedStudentName: "",
  nonSubmitted: new Set(),
  latestWorksheet: null,
};

const $ = (selector) => document.querySelector(selector);
let taskHideTimer = null;

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

function setTask(message, taskState = "active") {
  const indicator = $("#taskIndicator");
  if (!indicator) return;
  window.clearTimeout(taskHideTimer);
  indicator.hidden = false;
  indicator.dataset.state = taskState;
  $("#taskIndicatorLabel").textContent = message;
  $("#taskIndicatorState").textContent = taskState === "done" ? "Done" : taskState === "error" ? "Check" : "Active";
}

function settleTask(message = "Task complete", taskState = "done") {
  setTask(message, taskState);
  taskHideTimer = window.setTimeout(() => {
    const indicator = $("#taskIndicator");
    if (indicator) indicator.hidden = true;
  }, taskState === "error" ? 2200 : 1100);
}

function flashControl(element) {
  if (!element) return;
  element.classList.remove("is-activated");
  window.requestAnimationFrame(() => {
    element.classList.add("is-activated");
    window.setTimeout(() => element.classList.remove("is-activated"), 360);
  });
}

function setButtonBusy(target, busy) {
  const button = typeof target === "string" ? $(target) : target;
  if (!button) return;
  button.disabled = Boolean(busy);
  if (busy) {
    button.setAttribute("aria-busy", "true");
  } else {
    button.removeAttribute("aria-busy");
  }
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

function formatContentDate(value = "") {
  const match = String(value || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return value || "-";
  return `${match[3]}/${match[2]}/${match[1].slice(2)}`;
}

function contentDateLabel(item = {}) {
  if (item.date_missing || !item.date) return "Check date";
  return formatContentDate(item.date || "");
}

function contentPurpose(item = {}) {
  const purpose = item.purpose && typeof item.purpose === "object" ? item.purpose : {};
  const label = item.purpose_label || purpose.label || (item.kind ? item.kind : "Resource");
  return {
    id: item.purpose_id || purpose.id || "resource",
    label,
    tone: item.purpose_tone || purpose.tone || "resource",
    rank: Number(item.purpose_rank || purpose.rank || 90),
    trackable: Boolean(item.trackable ?? purpose.trackable),
  };
}

function contentSortDateValue(value = "") {
  const clean = String(value || "").trim();
  let match = clean.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) {
    return Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3]));
  }
  match = clean.match(/^(\d{1,2})[.\-_/ :](\d{1,2})[.\-_/ :](\d{2,4})$/);
  if (match) {
    const year = Number(match[3]) < 100 ? 2000 + Number(match[3]) : Number(match[3]);
    return Date.UTC(year, Number(match[2]) - 1, Number(match[1]));
  }
  return null;
}

function sortContentItems(items = []) {
  return [...items].sort((left, right) => {
    const leftDate = contentSortDateValue(left.date);
    const rightDate = contentSortDateValue(right.date);
    if (leftDate !== null || rightDate !== null) {
      if (leftDate === null) return 1;
      if (rightDate === null) return -1;
      if (leftDate !== rightDate) return rightDate - leftDate;
    }
    const a = [left.folder || "", left.title || "", left.path || ""].map((value) => String(value || "").toLowerCase());
    const b = [right.folder || "", right.title || "", right.path || ""].map((value) => String(value || "").toLowerCase());
    for (let index = 0; index < a.length; index += 1) {
      const compared = a[index].localeCompare(b[index]);
      if (compared) return compared;
    }
    return 0;
  });
}

function segmentMarkup(value, total = 12, tone = "accent") {
  const filled = Math.max(0, Math.min(total, Math.round(Number(value || 0))));
  return Array.from({ length: total }, (_, index) => `<span class="${index < filled ? `active ${tone}` : ""}"></span>`).join("");
}

function renderMissionTelemetry(data = {}) {
  const summary = data.summary || {};
  const students = data.student_summary || {};
  const concernCount = Number(students.concern_count || 0);
  const undatedCount = Number(summary.undated_folder_count || 0);
  const contents = Number(summary.content_item_count ?? summary.collection_candidate_count ?? 0);
  const classes = Number(summary.class_count ?? data.class_count ?? 0);
  const roster = Number(students.roster_count || 0);
  const readiness = concernCount || undatedCount ? "Action" : classes ? "Nominal" : "Standby";
  $("#missionReadiness").textContent = readiness;
  $("#missionReadiness").dataset.tone = concernCount || undatedCount ? "warn" : classes ? "ok" : "muted";
  $("#missionScanTime").textContent = shortDateTime(data.generated_at);
  $("#missionFollowUp").textContent = String(concernCount);
  $("#missionReadout").innerHTML = `
    <p>${classes} classes online · ${roster} students synced from Drive.</p>
    <p>${contents} filing items · ${Number(students.assignment_count || 0)} tracked submissions · ${Number(students.open_non_submission_count || 0)} open non-submissions.</p>
    ${undatedCount ? `<p>${undatedCount} folder${undatedCount === 1 ? "" : "s"} need date cleanup.</p>` : ""}
  `;
}

function renderSummary(data) {
  const summary = data?.summary || {};
  $("#classCount").textContent = String(summary.class_count ?? data?.class_count ?? "--");
  $("#lessonCount").textContent = String(summary.lesson_count ?? "--");
  $("#fileCount").textContent = String(summary.file_count ?? data?.file_count ?? "--");
  $("#collectCount").textContent = String(summary.content_item_count ?? summary.collection_candidate_count ?? "--");
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
            <div><span>Items</span><strong>${Number(item.content_item_count || 0)}</strong></div>
            <div><span>Watch</span><strong>${Number(item.student_report?.concern_count || 0)}</strong></div>
          </div>
          <div class="class-telemetry" aria-hidden="true">${segmentMarkup(Math.min(12, Number(item.lesson_count || 0)), 12, item.student_report?.concern_count ? "warn" : "accent")}</div>
          <button class="control-secondary" type="button" data-select-class="${escapeHtml(item.class)}">Open</button>
        </article>
      `).join("")
    : `<div class="empty">No class folders detected yet.</div>`;
}

function renderClassList(classes = []) {
  $("#classList").innerHTML = classes.map((item) => `
    <button type="button" class="control-quiet ${item.class === state.selectedClass ? "active" : ""}" data-select-class="${escapeHtml(item.class)}">
      <span>${escapeHtml(item.class)}</span>
      <strong>${Number(item.student_report?.concern_count || 0)}</strong>
    </button>
  `).join("");
}

function renderCollectionPanel(classItem) {
  const candidates = classItem?.collection_candidates || [];
  const undated = classItem?.undated_folders || [];
  const dateFlags = undated.length
    ? `
      <section class="date-check-panel">
        <strong>Check folder dates</strong>
        <p>These folders have no recognised date, so their contents sit at the bottom for now.</p>
        ${undated.map((folder) => `
          <article>
            <span>${escapeHtml(folder.folder || "Untitled folder")}</span>
            <small>${Number(folder.file_count || 0)} file${Number(folder.file_count || 0) === 1 ? "" : "s"}${folder.files?.length ? ` · ${escapeHtml(folder.files.join(", "))}` : ""}</small>
          </article>
        `).join("")}
      </section>
    `
    : "";
  const collections = candidates.length
    ? candidates.map((file) => `
        <article class="collection-item">
          <strong>${escapeHtml(file.name)}</strong>
          <small>${escapeHtml(file.path || "")}</small>
          <p>Detected as due work from the filename.</p>
        </article>
      `).join("")
    : `<div class="empty">No due-work signals detected from filenames yet.</div>`;
  $("#collectionPanel").innerHTML = `${dateFlags}${collections}`;
}

function renderContents(classItem) {
  if (!classItem) {
    $("#detailEyebrow").textContent = "Select class";
    $("#detailTitle").textContent = "Contents";
    $("#collectionPanel").innerHTML = "";
    $("#ledgerMeta").textContent = "--";
    $("#studentReport").innerHTML = `<div class="empty">Choose a class to load its roster.</div>`;
    $("#contentsTable").innerHTML = `<div class="empty">Choose a class to view its content page.</div>`;
    hideContentInspector();
    return;
  }
  $("#detailEyebrow").textContent = `${classItem.file_count || 0} files`;
  $("#detailTitle").textContent = `${classItem.class} Contents`;
  renderCollectionPanel(classItem);
  renderStudentReport(classItem.student_report || {});
  renderNonSubmissionRoster(classItem);
  const contentItems = sortContentItems(classItem.content_items || []);
  $("#contentsTable").innerHTML = `
    <div class="contents-row header">
      <div>No</div><div>Item</div><div>Tarikh</div>
    </div>
    ${contentItems.length ? contentItems.map((item, index) => {
      const statusBadge = item.no_submission_needed ? ` <span class="override-mark">no submission</span>` : "";
      const purpose = contentPurpose(item);
      return `
        <article class="contents-row ${item.date_missing || !item.date ? "date-missing" : ""}" data-purpose="${escapeHtml(purpose.id)}" data-track-lesson="${escapeHtml(item.date || "")}" data-track-topic="${escapeHtml(item.title || "")}" data-track-folder="${escapeHtml(item.folder || "")}" data-track-title="${escapeHtml(item.title || "")}" data-content-path="${escapeHtml(item.path || "")}" data-content-kind="${escapeHtml(item.kind || "")}" data-content-purpose-id="${escapeHtml(purpose.id)}" data-content-purpose-label="${escapeHtml(purpose.label)}" data-content-purpose-tone="${escapeHtml(purpose.tone)}" data-content-purpose-rank="${escapeHtml(String(purpose.rank))}" data-content-trackable="${purpose.trackable ? "1" : ""}" data-date-missing="${item.date_missing || !item.date ? "1" : ""}">
          <div class="contents-no"><strong>${index + 1}</strong></div>
          <div class="contents-title">
            <span class="content-purpose" data-tone="${escapeHtml(purpose.tone)}">${escapeHtml(purpose.label)}</span>
            <span>${escapeHtml(item.title || "Untitled")}${item.title_overridden ? ` <span class="override-mark">edited</span>` : ""}${statusBadge}</span>
          </div>
          <div class="contents-date">${escapeHtml(contentDateLabel(item))}</div>
        </article>
      `;
    }).join("") : `<div class="empty">No filing items detected yet for this class.</div>`}
  `;
  if (state.selectedContentItem?.path) {
    const stillExists = contentItems.some((item) => item.path === state.selectedContentItem.path);
    if (!stillExists) hideContentInspector();
  }
}

function contentItemFromRow(row) {
  return {
    date: row.dataset.trackLesson || "",
    folder: row.dataset.trackFolder || "",
    title: row.dataset.trackTitle || row.dataset.trackTopic || "",
    path: row.dataset.contentPath || "",
    kind: row.dataset.contentKind || "file",
    purpose_id: row.dataset.contentPurposeId || "resource",
    purpose_label: row.dataset.contentPurposeLabel || "Resource",
    purpose_tone: row.dataset.contentPurposeTone || "resource",
    purpose_rank: Number(row.dataset.contentPurposeRank || 90),
    trackable: row.dataset.contentTrackable === "1",
    date_missing: row.dataset.dateMissing === "1",
  };
}

function selectedContentRow(path) {
  if (!path) return null;
  return [...document.querySelectorAll(".contents-row[data-content-path]")]
    .find((row) => row.dataset.contentPath === path);
}

function showContentInspector(item = {}) {
  state.selectedContentItem = item;
  const inspector = $("#contentInspector");
  inspector.hidden = false;
  const reflectionPanel = $("#reflectionPanel");
  if (reflectionPanel) reflectionPanel.hidden = true;
  $("#contentTitleInput").value = item.title || "";
  const purpose = contentPurpose(item);
  $("#contentInspectorMeta").textContent = [
    purpose.label,
    item.kind || "file",
    item.path || "",
    item.date_missing || !item.date ? "Check folder date" : formatContentDate(item.date),
  ].filter(Boolean).join(" · ");
  document.querySelectorAll(".contents-row.is-selected").forEach((row) => row.classList.remove("is-selected"));
  selectedContentRow(item.path)?.classList.add("is-selected");
  prefillLessonFromItem(item, { focus: false });
  $("#contentTitleInput").focus();
}

function hideContentInspector() {
  state.selectedContentItem = null;
  state.latestWorksheet = null;
  const inspector = $("#contentInspector");
  if (inspector) inspector.hidden = true;
  const reflectionPanel = $("#reflectionPanel");
  if (reflectionPanel) reflectionPanel.hidden = true;
  document.querySelectorAll(".contents-row.is-selected").forEach((row) => row.classList.remove("is-selected"));
}

function updateContentItemInState(path, updates = {}) {
  const classItem = currentClassItem();
  if (!classItem || !path) return;
  classItem.content_items = (classItem.content_items || [])
    .map((item) => item.path === path ? { ...item, ...updates } : item)
    .filter((item) => !item.hidden);
  classItem.content_items = sortContentItems(classItem.content_items);
  classItem.content_item_count = classItem.content_items.length;
  if (state.data?.summary) {
    state.data.summary.content_item_count = (state.data.classes || [])
      .reduce((total, item) => total + Number(item.content_item_count || 0), 0);
  }
}

function markContentItemSubmissionNeeded(path, needed) {
  updateContentItemInState(path, { no_submission_needed: !needed });
  if (state.selectedContentItem?.path === path) {
    state.selectedContentItem = { ...state.selectedContentItem, no_submission_needed: !needed };
  }
}

function assignmentTimingText(assignment = {}) {
  const labels = (assignment.timing_context || [])
    .map((item) => item.label || "")
    .filter(Boolean);
  return labels.length ? ` · ${labels.map(escapeHtml).join(", ")}` : "";
}

function renderStudentReport(report = {}) {
  const students = report.students || [];
  const concerns = report.concerns || [];
  const insights = report.insights || [];
  const priorities = report.priority_items || [];
  const blindSpots = report.blind_spots || [];
  const feedForward = report.feed_forward_groups || [];
  const selectedStudent = students.find((student) => student.name === state.selectedStudentName) || concerns[0] || null;
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
      <article><span>Open non-sub</span><strong>${Number(report.open_non_submission_count || 0)}</strong></article>
      <article><span>Follow up</span><strong>${Number(report.concern_count || 0)}</strong></article>
    </div>
    <div class="submission-gauge" aria-label="Submission health">
      <div>${segmentMarkup(health, 12, report.concern_count ? "warn" : "success")}</div>
      <strong>${report.concern_count ? "Intervention path active" : "Submission channel clear"}</strong>
    </div>
    ${priorities.length ? `
      <section class="teacher-intel-panel">
        <div class="mini-section-head">
          <span>Priority list</span>
          <strong>${priorities.length}</strong>
        </div>
        ${priorities.map((item) => `
          <article data-tone="${escapeHtml(item.tone || "watch")}">
            <strong>${escapeHtml(item.title || "Priority")}</strong>
            <p>${escapeHtml(item.detail || "")}</p>
            <small>${escapeHtml(item.action || "")}</small>
          </article>
        `).join("")}
      </section>
    ` : ""}
    ${blindSpots.length ? `
      <section class="teacher-intel-panel blind-spots">
        <div class="mini-section-head">
          <span>Possible blind spots</span>
          <strong>${blindSpots.length}</strong>
        </div>
        ${blindSpots.map((item) => `
          <article data-tone="${escapeHtml(item.tone || "muted")}">
            <strong>${escapeHtml(item.title || "Blind spot")}</strong>
            <p>${escapeHtml(item.detail || "")}</p>
          </article>
        `).join("")}
      </section>
    ` : ""}
    ${feedForward.length ? `
      <section class="feed-forward-panel">
        <div class="mini-section-head">
          <span>Feed-forward groups</span>
          <strong>${feedForward.length}</strong>
        </div>
        <div class="feed-forward-grid">
          ${feedForward.map((group) => `
            <article>
              <div>
                <strong>${escapeHtml(group.label || "Group")}</strong>
                <span>${Number(group.count || 0)} student${Number(group.count || 0) === 1 ? "" : "s"}</span>
              </div>
              <p>${escapeHtml(group.move || "")}</p>
              <small>${escapeHtml((group.students || []).map((student) => student.name).join(", ") || group.trigger || "")}</small>
            </article>
          `).join("")}
        </div>
      </section>
    ` : ""}
    ${insights.length ? `
      <div class="insight-grid">
        ${insights.map((insight) => `
          <article class="insight-card" data-severity="${escapeHtml(insight.severity || "watch")}">
            <span>${escapeHtml(insight.kind || "signal")}</span>
            <strong>${escapeHtml(insight.title || "ClassOps signal")}</strong>
            <p>${escapeHtml(insight.detail || "")}</p>
          </article>
        `).join("")}
      </div>
    ` : ""}
    ${latest.length ? `
      <div class="assignment-tally">
        ${latest.map((assignment) => `
          <article data-assignment-id="${escapeHtml(assignment.id || "")}">
            <strong>${escapeHtml(assignment.assignment_title || "Tracked work")}</strong>
            <span>${Number(assignment.submitted_count || 0)}/${Number(assignment.roster_count || 0)} submitted · ${Number((assignment.non_submitted || []).length)} non-submission${Number((assignment.non_submitted || []).length) === 1 ? "" : "s"}${assignmentTimingText(assignment)}</span>
          </article>
        `).join("")}
      </div>
    ` : ""}
    <div class="student-table">
      <div class="student-row header">
        <div>Name</div><div>Done</div><div>Missing</div><div>Absent</div><div>Catch-up</div><div>Status</div>
      </div>
      ${visible.map((student) => `
        <article class="student-row ${selectedStudent?.name === student.name ? "is-selected" : ""}" data-status="${escapeHtml(student.status || "clear")}" data-student-name="${escapeHtml(student.name)}">
          <strong>${escapeHtml(student.name)}</strong>
          <div>${Number(student.submitted_count || 0)}</div>
          <div>${Number(student.missing_count || 0)}</div>
          <div>${Number(student.absent_count || 0)}</div>
          <div>${Number(student.catchup_count || 0)}</div>
          <div>${escapeHtml(student.risk_reasons?.[0] || student.status || "clear")}</div>
        </article>
      `).join("")}
    </div>
    ${selectedStudent ? `
      <section class="student-timeline-panel">
        <div class="mini-section-head">
          <span>Student timeline</span>
          <strong>${escapeHtml(selectedStudent.name || "")}</strong>
        </div>
        <div class="student-timeline">
          ${(selectedStudent.timeline || []).slice(-8).reverse().map((event) => `
            <article data-status="${escapeHtml(event.status || "clear")}">
              <strong>${escapeHtml(event.assignment_title || "Tracked work")}</strong>
              <span>${escapeHtml(event.status || "clear")}${event.collect_by ? ` · due ${escapeHtml(event.collect_by)}` : ""}</span>
            </article>
          `).join("") || `<div class="empty">No submission timeline yet for this student.</div>`}
          ${(selectedStudent.mark_flags || []).length ? `
            <article data-status="marks watch">
              <strong>Marks watch</strong>
              <span>${escapeHtml((selectedStudent.mark_flags || []).map((flag) => `${flag.label} ${flag.value}`).join(" · "))}</span>
            </article>
          ` : ""}
        </div>
      </section>
    ` : ""}
  `;
}

function currentClassItem() {
  return (state.data?.classes || []).find((item) => item.class === state.selectedClass);
}

function assignmentForContentItem(item = state.selectedContentItem, classItem = currentClassItem()) {
  const path = item?.path || "";
  if (!path) return null;
  return (classItem?.student_report?.assignments || [])
    .find((assignment) => (assignment.source_path || "") === path) || null;
}

function refreshStudentSummary() {
  if (!state.data?.student_summary) return;
  const reports = (state.data.classes || []).map((item) => item.student_report || {});
  state.data.student_summary.assignment_count = reports.reduce((total, report) => total + Number(report.assignment_count || 0), 0);
  state.data.student_summary.open_non_submission_count = reports.reduce((total, report) => total + Number(report.open_non_submission_count || 0), 0);
  state.data.student_summary.concern_count = reports.reduce((total, report) => total + Number(report.concern_count || 0), 0);
  state.data.student_summary.insight_count = reports.reduce((total, report) => total + Number(report.insight_count || 0), 0);
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

function prefillLessonFromItem(item = {}, { focus = true } = {}) {
  const date = item.date || "";
  state.selectedContentItem = item.path ? item : state.selectedContentItem;
  state.selectedFolder = item.folder || "";
  const existing = assignmentForContentItem(state.selectedContentItem);
  state.nonSubmitted = new Set(existing?.non_submitted || []);
  $("#lessonDateInput").value = /^\d{4}-\d{2}-\d{2}$/.test(date) ? date : "";
  $("#topicInput").value = item.title || "";
  const title = item.title || "";
  if (title) $("#assignmentTitleInput").value = title.trim();
  renderNonSubmissionRoster();
  if (focus) $("#assignmentTitleInput").focus();
}

function prefillLesson(button) {
  prefillLessonFromItem({
    date: button.dataset.trackLesson || "",
    folder: button.dataset.trackFolder || "",
    title: button.dataset.trackTitle || button.dataset.trackTopic || "",
  });
}

async function saveContentOverride({ hidden = null } = {}) {
  const item = state.selectedContentItem;
  if (!item?.path) return setStatus("Select a contents item first.", "warn");
  const title = $("#contentTitleInput").value.trim();
  if (!title && !hidden) return setStatus("Add a display title before saving.", "warn");
  setButtonBusy(hidden ? "#hideContentItemBtn" : "#saveContentTitleBtn", true);
  $(hidden ? "#saveContentTitleBtn" : "#hideContentItemBtn").disabled = true;
  setStatus(hidden ? "Hiding contents item..." : "Saving display title...");
  setTask(hidden ? "Hiding contents item" : "Saving display title");
  try {
    const payload = {
      path: item.path,
      title: hidden ? undefined : title,
      hidden,
    };
    const data = await api("/api/classops/content-override", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (hidden) {
      updateContentItemInState(item.path, { hidden: true });
      renderSummary(state.data);
      renderClassCards(state.data?.classes || []);
      renderClassList(state.data?.classes || []);
      renderContents(currentClassItem());
      setStatus("Item hidden from the printable contents page.", "ok");
      settleTask("Contents item hidden");
      return;
    }
    const nextTitle = data.override?.title || title;
    const updated = { ...item, title: nextTitle, title_overridden: true };
    updateContentItemInState(item.path, { title: nextTitle, title_overridden: true });
    renderContents(currentClassItem());
    showContentInspector(updated);
    setStatus("Display title saved.", "ok");
    settleTask("Display title saved");
  } catch (error) {
    setStatus(error.message, "error");
    settleTask("Content update failed", "error");
  } finally {
    setButtonBusy("#saveContentTitleBtn", false);
    setButtonBusy("#hideContentItemBtn", false);
  }
}

async function openSelectedContentFile() {
  const item = state.selectedContentItem;
  if (!item?.path) return setStatus("Select a contents item first.", "warn");
  setButtonBusy("#openContentFileBtn", true);
  setStatus("Opening Dropbox source file...");
  setTask("Requesting Dropbox file link");
  try {
    const data = await api(`/api/classops/dropbox/file-link?path=${encodeURIComponent(item.path)}`);
    if (!data.url) throw new Error("Dropbox did not return a file link.");
    window.open(data.url, "_blank", "noopener");
    setStatus("Opened source file in Dropbox.", "ok");
    settleTask("Dropbox file opened");
  } catch (error) {
    setStatus(error.message, "error");
    settleTask("Could not open Dropbox file", "error");
  } finally {
    setButtonBusy("#openContentFileBtn", false);
  }
}

function worksheetText(worksheet = {}) {
  const lines = [worksheet.summary || "Post-lesson reflection worksheet"];
  for (const section of worksheet.sections || []) {
    lines.push("", section.title || "Section");
    for (const prompt of section.prompts || []) lines.push(`- ${prompt}`);
  }
  return lines.join("\n");
}

function renderReflectionWorksheet(worksheet = {}) {
  state.latestWorksheet = worksheet;
  const panel = $("#reflectionPanel");
  if (!panel) return;
  panel.hidden = false;
  $("#reflectionTitle").textContent = worksheet.summary || "Post-lesson reflection worksheet";
  const meta = [
    worksheet.extracted ? "Extracted from lesson file" : "Metadata-based draft",
    worksheet.source_note || "",
    (worksheet.keywords || []).length ? `Keywords: ${(worksheet.keywords || []).slice(0, 5).join(", ")}` : "",
  ].filter(Boolean);
  $("#reflectionBody").innerHTML = `
    ${meta.length ? `<p class="reflection-meta">${escapeHtml(meta.join(" · "))}</p>` : ""}
    ${(worksheet.sections || []).map((section) => `
      <article>
        <strong>${escapeHtml(section.title || "Section")}</strong>
        <ul>${(section.prompts || []).map((prompt) => `<li>${escapeHtml(prompt)}</li>`).join("")}</ul>
      </article>
    `).join("")}
  `;
}

async function generateReflectionWorksheet() {
  const item = state.selectedContentItem;
  if (!state.selectedClass || !item?.path) return setStatus("Select a class lesson item first.", "warn");
  setButtonBusy("#reflectionWorksheetBtn", true);
  setStatus("Building post-lesson reflection worksheet...");
  setTask("Building reflection worksheet");
  try {
    const data = await api("/api/classops/reflection-worksheet", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        class_name: state.selectedClass,
        lesson: item,
      }),
    });
    renderReflectionWorksheet(data.worksheet || {});
    setStatus("Reflection worksheet drafted.", "ok");
    settleTask("Worksheet drafted");
  } catch (error) {
    setStatus(error.message, "error");
    settleTask("Worksheet generation failed", "error");
  } finally {
    setButtonBusy("#reflectionWorksheetBtn", false);
  }
}

async function copyReflectionWorksheet() {
  const text = worksheetText(state.latestWorksheet || {});
  if (!text.trim()) return setStatus("Generate a reflection worksheet first.", "warn");
  try {
    await navigator.clipboard.writeText(text);
    setStatus("Worksheet copied.", "ok");
  } catch (_) {
    setStatus("Could not copy automatically. Select the worksheet text manually.", "warn");
  }
}

function selectClass(className) {
  state.selectedClass = className;
  state.selectedFolder = "";
  state.selectedStudentName = "";
  hideContentInspector();
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
  setButtonBusy("#scanBtn", true);
  setStatus("Scanning Dropbox ClassOps folder...");
  setTask("Scanning Dropbox ClassOps folder");
  try {
    const data = await api("/api/classops/dashboard");
    renderDashboard(data);
    settleTask("Dropbox scan complete");
  } catch (error) {
    setStatus(error.message, "error");
    settleTask("Dropbox scan failed", "error");
  } finally {
    setButtonBusy("#scanBtn", false);
  }
}

$("#saveTokenBtn").addEventListener("click", () => {
  state.token = $("#tokenInput").value.trim();
  localStorage.setItem("hira_web_token", state.token);
  setStatus("Token saved. Ready to scan.", "ok");
});

$("#scanBtn").addEventListener("click", loadDashboard);
$("#printBtn").addEventListener("click", () => window.print());

function buildAssignmentPayload(nonSubmitted = [...state.nonSubmitted]) {
  if (!state.selectedClass) return setStatus("Select a class first.", "warn");
  if (!state.selectedContentItem?.path) {
    setStatus("Select an item from Contents before tracking submissions.", "warn");
    return null;
  }
  const payload = {
    class_name: state.selectedClass,
    lesson_date: $("#lessonDateInput").value,
    topic: $("#topicInput").value.trim(),
    folder: state.selectedFolder,
    source_path: state.selectedContentItem.path,
    assignment_title: $("#assignmentTitleInput").value.trim(),
    collect_by: $("#collectByInput").value,
    absent: [],
    submitted: [],
    non_submitted: nonSubmitted,
  };
  if (!payload.assignment_title) {
    setStatus("Add an assignment title before saving.", "warn");
    return null;
  }
  return payload;
}

function setTrackingButtonsDisabled(disabled, busyTarget = "") {
  ["#saveAssignmentBtn", "#allSubmittedBtn", "#noSubmissionNeededBtn"].forEach((selector) => {
    if (busyTarget === selector) {
      setButtonBusy(selector, disabled);
    } else {
      const button = $(selector);
      if (!button) return;
      button.disabled = disabled;
      if (!disabled) button.removeAttribute("aria-busy");
    }
  });
}

async function saveAssignmentTracking(nonSubmitted = [...state.nonSubmitted], label = "Tracking", busyTarget = "#saveAssignmentBtn") {
  const payload = buildAssignmentPayload(nonSubmitted);
  if (!payload) return;
  setTrackingButtonsDisabled(true, busyTarget);
  setStatus(`Saving ${state.selectedClass} tracking...`);
  setTask(`Saving ${state.selectedClass} submission tracking`);
  try {
    const result = await api("/api/classops/assignment", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const classItem = (state.data?.classes || []).find((item) => item.class === state.selectedClass);
    if (classItem) classItem.student_report = result.report;
    if (payload.source_path) markContentItemSubmissionNeeded(payload.source_path, true);
    refreshStudentSummary();
    renderMissionTelemetry(state.data || {});
    renderStudentReport(result.report || {});
    state.nonSubmitted = new Set(result.assignment?.non_submitted || []);
    renderNonSubmissionRoster(classItem);
    if (payload.source_path) renderContents(classItem);
    setStatus(`${label} saved with ${state.nonSubmitted.size} open non-submission${state.nonSubmitted.size === 1 ? "" : "s"}.`, "ok");
    settleTask(`${label} saved`);
  } catch (error) {
    setStatus(error.message, "error");
    settleTask("Submission tracking failed", "error");
  } finally {
    setTrackingButtonsDisabled(false);
  }
}

$("#assignmentForm").addEventListener("submit", async (event) => {
  event.preventDefault();
  await saveAssignmentTracking([...state.nonSubmitted], "Non-submission tracking");
});

$("#allSubmittedBtn").addEventListener("click", async () => {
  state.nonSubmitted = new Set();
  renderNonSubmissionRoster();
  await saveAssignmentTracking([], "All submitted", "#allSubmittedBtn");
});

$("#noSubmissionNeededBtn").addEventListener("click", async () => {
  if (!state.selectedClass) return setStatus("Select a class first.", "warn");
  const item = state.selectedContentItem;
  if (!item?.path) return setStatus("Select an item from Contents before marking no submission needed.", "warn");
  setTrackingButtonsDisabled(true, "#noSubmissionNeededBtn");
  setStatus("Marking item as no submission needed...");
  setTask("Marking item as no collection needed");
  try {
    const result = await api("/api/classops/assignment/no-submission-needed", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        class_name: state.selectedClass,
        source_path: item.path,
        assignment_title: $("#assignmentTitleInput").value.trim() || item.title || "",
      }),
    });
    const classItem = (state.data?.classes || []).find((entry) => entry.class === state.selectedClass);
    if (classItem) classItem.student_report = result.report;
    markContentItemSubmissionNeeded(item.path, false);
    refreshStudentSummary();
    renderMissionTelemetry(state.data || {});
    renderStudentReport(result.report || {});
    state.nonSubmitted = new Set();
    renderNonSubmissionRoster(classItem);
    renderContents(classItem);
    setStatus("Marked as no submission needed.", "ok");
    settleTask("No collection marker saved");
  } catch (error) {
    setStatus(error.message, "error");
    settleTask("No collection update failed", "error");
  } finally {
    setTrackingButtonsDisabled(false);
  }
});

$("#clearNonSubmissionBtn").addEventListener("click", () => {
  state.nonSubmitted = new Set();
  renderNonSubmissionRoster();
});

$("#closeContentInspectorBtn").addEventListener("click", hideContentInspector);
$("#saveContentTitleBtn").addEventListener("click", () => saveContentOverride());
$("#hideContentItemBtn").addEventListener("click", () => saveContentOverride({ hidden: true }));
$("#openContentFileBtn").addEventListener("click", openSelectedContentFile);
$("#reflectionWorksheetBtn").addEventListener("click", generateReflectionWorksheet);
$("#copyReflectionBtn").addEventListener("click", copyReflectionWorksheet);
$("#trackContentItemBtn").addEventListener("click", () => {
  if (!state.selectedContentItem) return setStatus("Select a contents item first.", "warn");
  prefillLessonFromItem(state.selectedContentItem);
  setStatus("Loaded item into submission tracker.", "ok");
});

document.addEventListener("pointerdown", (event) => {
  const control = event.target.closest("button, .ghost, .student-chip, .contents-row[data-content-path], .student-row[data-student-name]");
  if (!control || control.matches(":disabled")) return;
  control.classList.add("is-pressing");
});

document.addEventListener("pointerup", (event) => {
  const control = event.target.closest("button, .ghost, .student-chip, .contents-row[data-content-path], .student-row[data-student-name]");
  if (!control) return;
  control.classList.remove("is-pressing");
  flashControl(control);
});

document.addEventListener("pointercancel", () => {
  document.querySelectorAll(".is-pressing").forEach((item) => item.classList.remove("is-pressing"));
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

$("#studentReport").addEventListener("click", (event) => {
  const row = event.target.closest("[data-student-name]");
  if (!row) return;
  state.selectedStudentName = row.dataset.studentName || "";
  const report = currentClassItem()?.student_report || {};
  renderStudentReport(report);
});

document.addEventListener("click", (event) => {
  const contentRow = event.target.closest(".contents-row[data-content-path]");
  if (contentRow && !contentRow.classList.contains("header")) {
    showContentInspector(contentItemFromRow(contentRow));
    return;
  }
  const lessonButton = event.target.closest("[data-track-lesson]:not(.contents-row)");
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
