const escapeHtml = (value) => String(value ?? "")
  .replaceAll("&", "&amp;")
  .replaceAll("<", "&lt;")
  .replaceAll(">", "&gt;")
  .replaceAll('"', "&quot;")
  .replaceAll("'", "&#039;");

export const CONNECTIONS = [
  {
    key: "calendar",
    label: "Calendar",
    icon: "calendar",
    setup: "Add Google credentials in Railway, then grant that account access to the calendars H.I.R.A should read.",
    variables: "GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_CALENDAR_IDS",
  },
  {
    key: "google",
    label: "Google Tasks and Sheets",
    icon: "sparkles",
    setup: "Add Google credentials and the main Sheet ID in Railway. Share the sheet with the configured Google account.",
    variables: "GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SHEET_ID",
  },
  {
    key: "work_drive",
    label: "Work Google Drive",
    icon: "folder",
    setup: "Connect the work Google account with Sheets and Drive access, then confirm it can edit the required files.",
    variables: "GOOGLE_WORK_SHEETS_REFRESH_TOKEN",
  },
  {
    key: "personal_gmail",
    label: "Personal Gmail",
    icon: "mail",
    setup: "Create a Gmail OAuth refresh token for the personal account and add it with the matching client ID and secret.",
    variables: "GOOGLE_GMAIL_REFRESH_TOKEN, GOOGLE_GMAIL_CLIENT_ID, GOOGLE_GMAIL_CLIENT_SECRET",
  },
  {
    key: "personal_gmail2",
    label: "Personal Gmail 2",
    icon: "mail-plus",
    setup: "Create a Gmail OAuth refresh token for the second personal account. The main Gmail client can be reused.",
    variables: "GOOGLE_GMAIL2_REFRESH_TOKEN",
  },
  {
    key: "work_gmail",
    label: "Work Gmail",
    icon: "briefcase-business",
    setup: "Create a fresh work Gmail OAuth token. If it was previously connected, replace the revoked or expired refresh token.",
    variables: "GOOGLE_WORK_GMAIL_REFRESH_TOKEN",
  },
  {
    key: "dropbox",
    label: "Dropbox and ClassOps",
    icon: "archive",
    setup: "Add the Dropbox app credentials and refresh token, then set the ClassOps root folder if it differs from the default.",
    variables: "DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN",
  },
];

export function integrationHealthView(services = {}) {
  const rows = CONNECTIONS.map((connection) => {
    const connected = Boolean(services[connection.key]);
    const detail = services?._details?.[connection.key] || {};
    const state = String(detail.state || (connected ? "on" : "off")).toLowerCase();
    const stateLabel = detail.label || (connected ? "Connected" : "Needs setup");
    const guidance = detail.setup || connection.setup;
    const lastRun = String(detail.last_run || "").trim();
    return `
      <details class="status-row integration-status-row" data-state="${escapeHtml(state)}">
        <summary>
          <span>${escapeHtml(connection.label)}</span>
          <strong>${escapeHtml(stateLabel)}</strong>
        </summary>
        <div class="integration-setup-copy">
          <p>${escapeHtml(connected ? "Connection is available. Expand this only when credentials need to be replaced." : guidance)}</p>
          ${lastRun ? `<small>Last provider check: ${escapeHtml(lastRun)}</small>` : ""}
          <small>Railway variables: <code>${escapeHtml(connection.variables)}</code></small>
        </div>
      </details>
    `;
  }).join("");
  const coreReady = Boolean(services.google || services.calendar);
  return {
    rows,
    coreReady,
    summary: coreReady
      ? "Core calendar and task sources are connected. Expand any optional service that still needs attention."
      : "Core calendar and task sources are disconnected. Expand a service below for the exact setup steps.",
  };
}
