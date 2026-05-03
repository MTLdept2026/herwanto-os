/* H.I.R.A Growth Log — renderer (Nothing OS edition) */

const $ = (s) => document.querySelector(s);

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
  const div = el("div", "hero-stat");
  div.append(el("span", "", label));
  div.append(el("strong", "", value));
  return div;
}

/* Nothing-style score: pixel number + dot-segment bar */
function ringCard(item) {
  const card = el("article", "ring-card");

  const wrap = el("div", "ring-score-wrap");
  wrap.append(el("div", "ring-score-value", `${item.score}`));

  const dots = el("div", "ring-dots");
  const total = 20;
  const filled = Math.round((item.score / 100) * total);
  for (let i = 0; i < total; i++) {
    dots.append(el("span", i < filled ? "dot dot--on" : "dot dot--off"));
  }
  wrap.append(dots);
  card.append(wrap);

  card.append(el("h3", "", item.name));
  card.append(el("p", "", item.description));
  return card;
}

function timelineCard(chapter, index) {
  const card = el("article", "timeline-card");

  /* ── meta column */
  const meta = el("div", "timeline-meta");
  meta.append(el("div", "timeline-date", chapter.date.slice(5).replace("-", ".")));
  meta.append(el("p", "eyebrow", chapter.era));
  meta.append(el("span", "timeline-tag", chapter.tag));
  card.append(meta);

  /* ── story column */
  const story = el("div", "timeline-story");
  story.append(el("h3", "", chapter.title));
  story.append(el("p", "", chapter.summary));
  const list = el("ul");
  (chapter.details || []).forEach((d) => list.append(el("li", "", d)));
  story.append(list);
  card.append(story);

  /* ── impact column */
  const impact = el("aside", "impact-box");
  impact.append(
    el("strong", "", `BUILD ${String(index + 1).padStart(2, "0")} · ${chapter.commit}`)
  );
  impact.append(el("p", "", chapter.impact));
  card.append(impact);

  return card;
}

function render(data) {
  document.title = data.name || "H.I.R.A Growth Log";

  const subtitle = $("#growthSubtitle");
  if (subtitle) subtitle.textContent = data.subtitle;

  const stage = $("#currentStage");
  if (stage) stage.textContent = data.summary?.currentStage || "—";

  const stats = $("#heroStats");
  if (stats) {
    stats.replaceChildren(
      heroStat("STARTED",  data.summary?.start           || "—"),
      heroStat("UPDATED",  data.summary?.current         || "—"),
      heroStat("COMMITS",  String(data.summary?.commitsTracked || "—")),
      heroStat("PHASES",   String(data.summary?.majorPhases   || "—"))
    );
  }

  const metrics = $("#growthMetrics");
  if (metrics) metrics.replaceChildren(...(data.metrics || []).map(metricCard));

  const rings = $("#capabilityRings");
  if (rings) rings.replaceChildren(...(data.capabilityRings || []).map(ringCard));

  const timeline = $("#timelineList");
  if (timeline) {
    const chapters = Array.isArray(data.chapters) ? [...data.chapters].reverse() : [];
    timeline.replaceChildren(...chapters.map(timelineCard));
  }

  const caps = $("#currentCapabilities");
  if (caps) caps.replaceChildren(...(data.currentCapabilities || []).map((t) => el("li", "", t)));

  const protocol = $("#updateProtocol");
  if (protocol) protocol.replaceChildren(...(data.updateProtocol || []).map((t) => el("li", "", t)));
}

async function loadGrowth() {
  try {
    const r = await fetch("/static/hira-growth-data.json", { cache: "no-store" });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    render(await r.json());
  } catch (err) {
    const sub = $("#growthSubtitle");
    if (sub) sub.textContent = `LOAD ERROR: ${err.message}`;
  }
}

loadGrowth();
