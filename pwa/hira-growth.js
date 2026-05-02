const $ = (selector) => document.querySelector(selector);

function setText(selector, value) {
  const el = $(selector);
  if (el) el.textContent = value;
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function metricCard(metric) {
  const card = el("article", "metric-card");
  card.append(el("span", "", metric.label));
  card.append(el("strong", "", metric.value));
  card.append(el("p", "", metric.note));
  return card;
}

function heroStat(label, value) {
  const card = el("div", "hero-stat");
  card.append(el("span", "", label));
  card.append(el("strong", "", value));
  return card;
}

function ringCard(item) {
  const card = el("article", "ring-card");
  const score = el("div", "ring-score", `${item.score}`);
  score.style.setProperty("--score", item.score);
  card.append(score);
  card.append(el("h3", "", item.name));
  card.append(el("p", "", item.description));
  return card;
}

function timelineCard(chapter, index) {
  const card = el("article", "timeline-card");
  const meta = el("div", "timeline-meta");
  meta.append(el("div", "timeline-date", chapter.date.slice(5).replace("-", ".")));
  meta.append(el("p", "eyebrow", chapter.era));
  meta.append(el("span", "timeline-tag", chapter.tag));

  const story = el("div", "timeline-story");
  story.append(el("h3", "", chapter.title));
  story.append(el("p", "", chapter.summary));
  const list = el("ul");
  (chapter.details || []).forEach((detail) => list.append(el("li", "", detail)));
  story.append(list);

  const impact = el("aside", "impact-box");
  impact.append(el("strong", "", `Build ${String(index + 1).padStart(2, "0")} / ${chapter.commit}`));
  impact.append(el("p", "", chapter.impact));

  card.append(meta, story, impact);
  return card;
}

function render(data) {
  document.title = data.name || "H.I.R.A Growth Log";
  setText("#growthSubtitle", data.subtitle);
  setText("#currentStage", data.summary?.currentStage || "Current core");

  const heroStats = $("#heroStats");
  heroStats.replaceChildren(
    heroStat("Started", data.summary?.start || "--"),
    heroStat("Current", data.summary?.current || "--"),
    heroStat("Commits", String(data.summary?.commitsTracked || "--")),
    heroStat("Phases", String(data.summary?.majorPhases || "--"))
  );

  $("#growthMetrics").replaceChildren(...(data.metrics || []).map(metricCard));
  $("#capabilityRings").replaceChildren(...(data.capabilityRings || []).map(ringCard));
  $("#timelineList").replaceChildren(...(data.chapters || []).map(timelineCard));

  const capabilities = $("#currentCapabilities");
  capabilities.replaceChildren(...(data.currentCapabilities || []).map((item) => el("li", "", item)));

  const protocol = $("#updateProtocol");
  protocol.replaceChildren(...(data.updateProtocol || []).map((item) => el("li", "", item)));
}

async function loadGrowth() {
  try {
    const response = await fetch("/static/hira-growth-data.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
  } catch (error) {
    setText("#growthSubtitle", `Could not load growth log: ${error.message}`);
  }
}

loadGrowth();
