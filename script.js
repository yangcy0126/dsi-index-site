const state = {
  summary: null,
  trump: null,
  trumpDirected: null,
  visitors: null,
  trumpSeriesMode: "7d",
  trumpDirectedSeriesMode: "30d",
  trumpDirectedSelectedCode: null,
  trumpDirectedCache: new Map(),
};

const summaryPath = "data/summary.json";
const trumpPath = "data/trump_indices.json";
const trumpDirectedSummaryPath = "data/trump_directed_summary.json";
const trumpDirectedBasePath = "data/trump_directed";
const trumpDataVersion = "20261121";
const visitorsPath = "data/visitor_stats.json";
const assetVersion = "20260420-dsi-8";
const VISITORS_REFRESH_INTERVAL_MS = 120000;
let visitorsRefreshTimer = null;
let visitorsRefreshInFlight = false;
const TRUMP_POLITICAL_EVENTS = [
  { date: "2015-06-16", label: "Campaign launch", chartLabel: "Campaign launch", chartY: 2.95, chartAnchor: "bottom" },
  { date: "2016-11-08", label: "Wins 2016 election", chartLabel: "2016 win", chartY: 2.55, chartAnchor: "bottom" },
  { date: "2017-01-20", label: "First inauguration", chartLabel: "1st inauguration", chartY: 2.15, chartAnchor: "bottom" },
  { date: "2019-12-18", label: "First impeachment", chartLabel: "1st impeachment", chartY: -2.15, chartAnchor: "top" },
  { date: "2020-11-03", label: "2020 election", chartLabel: "2020 election", chartY: -2.95, chartAnchor: "top" },
  { date: "2021-01-06", label: "Jan 6 Capitol riot", chartLabel: "Capitol riot", chartY: -2.45, chartAnchor: "top" },
  { date: "2022-11-15", label: "2024 campaign launch", chartLabel: "2024 launch", chartY: 2.95, chartAnchor: "bottom" },
  { date: "2024-07-13", label: "Butler rally shooting", chartLabel: "Butler shooting", chartY: 2.55, chartAnchor: "bottom" },
  { date: "2024-11-05", label: "Wins 2024 election", chartLabel: "2024 win", chartY: 2.15, chartAnchor: "bottom" },
  { date: "2025-01-20", label: "Second inauguration", chartLabel: "2nd inauguration", chartY: -2.15, chartAnchor: "top" },
];

function versionedPath(path, version = assetVersion) {
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}v=${encodeURIComponent(version)}`;
}

function formatDate(dateText) {
  if (!dateText) {
    return "--";
  }
  const date = new Date(`${dateText}T00:00:00`);
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function hasNumericValue(value) {
  return value !== null && value !== undefined && !Number.isNaN(Number(value));
}

function isPlaceholderCountry(country) {
  return Boolean(country?.is_placeholder);
}

function countryFlagMarkup(code, extraClass = "") {
  const flagCode = code === "UK" ? "gb" : String(code || "").toLowerCase();
  const className = ["country-flag", extraClass].filter(Boolean).join(" ");
  return `<img class="${className}" src="assets/flags/${flagCode}.svg" alt="" aria-hidden="true" width="26" height="18" loading="lazy" decoding="async">`;
}

function formatScore(value, digits = 3) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "--";
  }
  const rounded = Number(value).toFixed(digits);
  return rounded.replace(/\.?0+$/, "");
}

function formatWholeNumber(value) {
  if (!hasNumericValue(value)) {
    return "--";
  }
  return Number(value).toLocaleString("en-US");
}

function trumpSeriesLabel(mode) {
  if (mode === "daily") {
    return "daily";
  }
  if (mode === "30d") {
    return "30-day smoothed";
  }
  return "7-day smoothed";
}

function getTrumpSeriesKey(baseKey) {
  if (state.trumpSeriesMode === "daily") {
    return baseKey;
  }
  if (state.trumpSeriesMode === "30d") {
    return `${baseKey}_30d`;
  }
  return `${baseKey}_7d`;
}

function getLatestTrumpRecord() {
  const records = state.trump?.records || [];
  return records.length ? records[records.length - 1] : null;
}

function getTrumpDirectedCountryMeta(code) {
  return (state.trumpDirected?.countries || []).find((country) => country.code === code) || null;
}

function getLatestTrumpDirectedRecord(records) {
  return records.length ? records[records.length - 1] : null;
}

function getTrumpDirectedSeriesKey(baseKey) {
  if (state.trumpDirectedSeriesMode === "daily") {
    return baseKey;
  }
  if (state.trumpDirectedSeriesMode === "30d") {
    return `${baseKey}_30d`;
  }
  return `${baseKey}_7d`;
}

function getTrumpDirectedBoardSnapshot(country) {
  const value = country.latest_tone_30d;
  const tone = hasNumericValue(value)
    ? trumpToneMeta(value)
    : { label: "no recent 30-day signal", className: "tone-muted" };
  return { value, tone };
}

function directedAttentionMeta(score) {
  if (!hasNumericValue(score)) {
    return { label: "unavailable", className: "tone-muted" };
  }
  if (score >= 1) {
    return { label: "very active", className: "signal-hot" };
  }
  if (score >= 0.35) {
    return { label: "active", className: "signal-hot" };
  }
  if (score > 0) {
    return { label: "light attention", className: "tone-neutral" };
  }
  return { label: "quiet", className: "signal-cool" };
}

function getVisibleTrumpPoliticalEvents(records) {
  if (!records.length) {
    return [];
  }
  const start = records[0].date;
  const end = records[records.length - 1].date;
  return TRUMP_POLITICAL_EVENTS.filter((event) => event.date >= start && event.date <= end);
}

function trumpToneMeta(score) {
  if (!hasNumericValue(score)) {
    return { label: "unavailable", className: "tone-muted" };
  }
  if (score <= -1.5) {
    return { label: "highly hostile", className: "tone-negative" };
  }
  if (score <= -0.5) {
    return { label: "pressure-heavy", className: "tone-negative" };
  }
  if (score < 0.5) {
    return { label: "mixed or neutral", className: "tone-neutral" };
  }
  if (score < 1.5) {
    return { label: "positive", className: "tone-positive" };
  }
  return { label: "triumphal", className: "tone-positive" };
}

function trumpGeopoliticalMeta(score) {
  if (!hasNumericValue(score)) {
    return { label: "unavailable", className: "tone-muted" };
  }
  if (score <= -1.5) {
    return { label: "strongly escalatory", className: "tone-negative" };
  }
  if (score <= -0.5) {
    return { label: "escalatory", className: "tone-negative" };
  }
  if (score < 0.5) {
    return { label: "descriptive or balanced", className: "tone-neutral" };
  }
  if (score < 1.5) {
    return { label: "de-escalatory", className: "tone-positive" };
  }
  return { label: "strongly de-escalatory", className: "tone-positive" };
}

function trumpShockMeta(score) {
  if (!hasNumericValue(score)) {
    return { label: "unavailable", className: "tone-muted" };
  }
  if (score >= 1.5) {
    return { label: "extremely elevated", className: "signal-hot" };
  }
  if (score >= 0.5) {
    return { label: "elevated", className: "signal-hot" };
  }
  if (score > -0.5) {
    return { label: "contained", className: "tone-neutral" };
  }
  return { label: "subdued", className: "signal-cool" };
}

function getCountryByCode(code) {
  return state.summary.countries.find((country) => country.code === code);
}

async function fetchJson(path, version = assetVersion) {
  const response = await fetch(versionedPath(path, version), { cache: "no-cache" });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${path}`);
  }
  return response.json();
}

async function refreshVisitorsCardLive() {
  if (visitorsRefreshInFlight) {
    return;
  }
  if (document.hidden) {
    return;
  }

  visitorsRefreshInFlight = true;
  try {
    state.visitors = await fetchJson(
      visitorsPath,
      `${assetVersion}-live-${Math.floor(Date.now() / VISITORS_REFRESH_INTERVAL_MS)}`,
    );
    renderVisitorsCard();
  } catch (error) {
    console.warn("Visitor card refresh failed", error);
  } finally {
    visitorsRefreshInFlight = false;
  }
}

function startVisitorsAutoRefresh() {
  if (visitorsRefreshTimer !== null) {
    return;
  }
  visitorsRefreshTimer = window.setInterval(() => {
    void refreshVisitorsCardLive();
  }, VISITORS_REFRESH_INTERVAL_MS);
}

async function loadTrumpDirectedCountryData(code) {
  if (!state.trumpDirectedCache.has(code)) {
    state.trumpDirectedCache.set(code, fetchJson(`${trumpDirectedBasePath}/${code}.json`, state.trumpDirected.generated_at || trumpDataVersion));
  }
  return state.trumpDirectedCache.get(code);
}

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (character) => {
    const entities = {
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    };
    return entities[character] || character;
  });
}

function renderVisitorsCard() {
  const title = document.querySelector("#visitors-card .visitors-title");
  const stats = document.getElementById("visitors-stats");
  const list = document.getElementById("visitors-list");
  if (!title || !stats || !list) {
    return;
  }

  const snapshot = state.visitors;
  if (!snapshot?.available) {
    title.hidden = false;
    title.textContent = "Visitor snapshot unavailable";
    stats.innerHTML = `
      <div class="visitor-stat">
        <span class="visitor-stat-label">Status</span>
        <span class="visitor-stat-value">Retry next build</span>
      </div>
    `;
    list.innerHTML = `
      <div class="visitors-empty">
        The custom visitor card will refresh automatically once the public counter snapshot is reachable again.
      </div>
    `;
    return;
  }

  title.hidden = true;
  title.textContent = "";
  stats.innerHTML = `
    <div class="visitor-stat">
      <span class="visitor-stat-label">Visitors yesterday</span>
      <span class="visitor-stat-value">${formatWholeNumber(snapshot.visitors_yesterday)}</span>
    </div>
    <div class="visitor-stat">
      <span class="visitor-stat-label">Total<br>Views</span>
      <span class="visitor-stat-value">${formatWholeNumber(snapshot.total_views)}</span>
    </div>
  `;

  const displayCountries = snapshot.display_countries || snapshot.top_countries || [];
  list.innerHTML = displayCountries.length
    ? displayCountries.map((country) => `
      <div class="visitor-row">
        <span class="visitor-code">${escapeHtml(country.code)}</span>
        <span class="visitor-country">${escapeHtml(country.country)}</span>
        <span class="visitor-count">${formatWholeNumber(country.visitors)}</span>
      </div>
    `).join("")
    : `
      <div class="visitors-empty">
        No visitor rows are available for display in the current snapshot.
      </div>
    `;
}

function renderLegacyDownloadList() {
  const list = document.getElementById("download-list");
  list.innerHTML = "";

  const masterDownloads = [
    {
      title: "Full daily dataset",
      meta: "Excel workbook with the full calendar-day panel and a variable-definitions sheet",
      href: "data/wdsi_all_countries.xlsx",
    },
    {
      title: "Site summary (JSON)",
      meta: "Country list, latest values, coverage windows, and download paths",
      href: summaryPath,
    },
    {
      title: "Event markers (JSON)",
      meta: "Public event list for annotating charts with key dates",
      href: eventsPath,
    },
  ];

  const countryDownloads = state.summary.countries.flatMap((country) => [
    {
      title: `${country.label} data`,
      meta: isPlaceholderCountry(country)
        ? `${country.code} - placeholder slot, CSV pending`
        : `${country.code} - ${country.publication_days} publication days`,
      href: isPlaceholderCountry(country) ? null : country.file_xlsx || country.file_csv,
    },
    {
      title: `${country.label} data (JSON)`,
      meta: isPlaceholderCountry(country)
        ? "Placeholder metadata only; time series will appear after onboarding"
        : "Useful for direct use in web apps or scripts",
      href: isPlaceholderCountry(country) ? null : country.file_json,
    },
  ]);

  [...masterDownloads, ...countryDownloads].forEach((item) => {
    const row = document.createElement("div");
    row.className = `download-item ${item.href === "data/wdsi_all_countries.xlsx" ? "download-item-wide" : ""}`.trim();
    row.innerHTML = `
      <div>
        <strong>${item.title}</strong>
        <div class="download-meta">${item.meta}</div>
      </div>
      ${item.href
        ? `<a class="download-link" href="${item.href}" target="_blank" rel="noreferrer">Download</a>`
        : '<span class="download-link is-disabled">Pending</span>'}
    `;
    list.appendChild(row);
  });
}

function renderTrumpSupplementMeta() {
  const section = document.getElementById("trump-supplement");
  if (!state.trump || !(state.trump.records || []).length) {
    section.hidden = true;
    return;
  }

  section.hidden = false;
  const latest = getLatestTrumpRecord();
  const coverage = state.trump.scoring_coverage || {};

  document.getElementById("trump-coverage-range").textContent =
    `${formatDate(state.trump.coverage_start)} to ${formatDate(state.trump.coverage_end)}`;
  document.getElementById("trump-latest-date").textContent = formatDate(latest?.date);
  document.getElementById("trump-score-coverage").textContent =
    `${formatWholeNumber(coverage.accepted_scored_posts)} / ${formatWholeNumber(coverage.authored_text_posts)}`;
  document.getElementById("trump-platforms").textContent = "Twitter + Truth Social";
  document.getElementById("trump-mode-note").textContent =
    `Current view: ${trumpSeriesLabel(state.trumpSeriesMode)}. The shaded band marks the platform transition gap from Jan 9, 2021 to Feb 13, 2022.`;
}

function renderTrumpMetricCards() {
  const latest = getLatestTrumpRecord();
  if (!latest) {
    return;
  }

  const toneKey = getTrumpSeriesKey("trump_tone_index");
  const geoKey = getTrumpSeriesKey("trump_geopolitical_index");
  const shockKey = getTrumpSeriesKey("trump_shock_index");
  const toneValue = Number(latest[toneKey]);
  const geoValue = Number(latest[geoKey]);
  const shockValue = Number(latest[shockKey]);
  const tone = trumpToneMeta(toneValue);
  const geo = trumpGeopoliticalMeta(geoValue);
  const shock = trumpShockMeta(shockValue);
  const modeLabel = trumpSeriesLabel(state.trumpSeriesMode);
  const noScoredText = state.trumpSeriesMode === "daily" && state.trump.latest_scored_authored_posts === 0;

  const toneEl = document.getElementById("trump-tone-latest");
  toneEl.textContent = formatScore(toneValue);
  toneEl.className = `metric-value ${tone.className}`;
  document.getElementById("trump-tone-caption").textContent =
    noScoredText ? "No scored original text on this date; the daily value is zero by convention." :
    `${modeLabel} average across scored original text posts. Latest reading suggests ${tone.label} rhetoric.`;

  const geoEl = document.getElementById("trump-geo-latest");
  geoEl.textContent = formatScore(geoValue);
  geoEl.className = `metric-value ${geo.className}`;
  document.getElementById("trump-geo-caption").textContent =
    noScoredText ? "No scored original text on this date; the daily value is zero by convention." :
    `${modeLabel} mean across scored original text posts. Non-geopolitical posts contribute zero. Latest stance is ${geo.label}.`;

  const shockEl = document.getElementById("trump-shock-latest");
  shockEl.textContent = formatScore(shockValue);
  shockEl.className = `metric-value ${shock.className}`;
  document.getElementById("trump-shock-caption").textContent =
    `${modeLabel} composite of post volume, intensity, all-caps, exclamation marks, and reblog density. Latest reading is ${shock.label}.`;
}

function buildTrumpChartLayout(records) {
  const politicalEvents = getVisibleTrumpPoliticalEvents(records);

  return {
    margin: { l: 46, r: 24, t: 36, b: 44 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(255,255,255,0)",
    hovermode: "x unified",
    font: {
      family: '"Source Sans 3", Arial, sans-serif',
      color: "#18212b",
    },
    xaxis: {
      showgrid: true,
      gridcolor: "rgba(24, 33, 43, 0.10)",
      zeroline: false,
      tickfont: { color: "#5c6672" },
    },
    yaxis: {
      title: "Trump index",
      range: [-3.2, 3.2],
      showgrid: true,
      gridcolor: "rgba(24, 33, 43, 0.10)",
      zeroline: true,
      zerolinecolor: "rgba(24, 33, 43, 0.28)",
      tickfont: { color: "#5c6672" },
    },
    legend: {
      orientation: "h",
      yanchor: "bottom",
      y: 1.02,
      xanchor: "left",
      x: 0,
    },
    shapes: [
      {
        type: "rect",
        x0: "2021-01-09",
        x1: "2022-02-13",
        y0: -3.2,
        y1: 3.2,
        line: { width: 0 },
        fillcolor: "rgba(24, 33, 43, 0.06)",
      },
      ...politicalEvents.map((event) => ({
        type: "line",
        x0: event.date,
        x1: event.date,
        y0: -3.2,
        y1: 3.2,
        line: {
          color: "rgba(150, 59, 52, 0.24)",
          width: 1.2,
          dash: "dot",
        },
      })),
    ],
    annotations: [
      {
        x: "2021-07-15",
        y: 2.75,
        xref: "x",
        yref: "y",
        text: "platform gap",
        showarrow: false,
        font: { size: 11, color: "#5c6672" },
      },
      ...politicalEvents.map((event) => ({
        x: event.date,
        y: event.chartY ?? 2.75,
        xref: "x",
        yref: "y",
        text: event.chartLabel || event.label,
        showarrow: false,
        xanchor: "center",
        yanchor: event.chartAnchor || "bottom",
        align: "center",
        bgcolor: "rgba(255, 248, 242, 0.92)",
        bordercolor: "rgba(150, 59, 52, 0.20)",
        borderwidth: 1,
        borderpad: 4,
        font: { size: 10, color: "#743c37" },
      })),
    ],
  };
}

function renderTrumpChart() {
  const records = state.trump?.records || [];
  if (!records.length) {
    return;
  }

  const toneKey = getTrumpSeriesKey("trump_tone_index");
  const geoKey = getTrumpSeriesKey("trump_geopolitical_index");
  const shockKey = getTrumpSeriesKey("trump_shock_index");
  const dates = records.map((record) => record.date);

  const traces = [
    {
      x: dates,
      y: records.map((record) => record[toneKey]),
      type: "scatter",
      mode: "lines",
      name: "Trump Tone Index",
      line: {
        color: "#963b34",
        width: 2.6,
      },
      hovertemplate: "%{x}<br>Trump Tone Index: %{y:.3f}<extra></extra>",
    },
    {
      x: dates,
      y: records.map((record) => record[geoKey]),
      type: "scatter",
      mode: "lines",
      name: "Trump Geopolitical Index",
      line: {
        color: "#1f4e79",
        width: 2.4,
      },
      hovertemplate: "%{x}<br>Trump Geopolitical Index: %{y:.3f}<extra></extra>",
    },
    {
      x: dates,
      y: records.map((record) => record[shockKey]),
      type: "scatter",
      mode: "lines",
      name: "Trump Shock Index",
      line: {
        color: "#343e49",
        width: 2.2,
      },
      hovertemplate: "%{x}<br>Trump Shock Index: %{y:.3f}<extra></extra>",
    },
  ];

  Plotly.react("trump-chart", traces, buildTrumpChartLayout(records), {
    displayModeBar: true,
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  });

  document.getElementById("trump-chart-caption").textContent =
    `This panel overlays the ${trumpSeriesLabel(state.trumpSeriesMode)} Trump Tone Index, Trump Geopolitical Index, and Trump Shock Index across Twitter and Truth Social. Dotted markers flag major Trump political milestones, while the shaded band marks the platform transition gap. Levels are complementary to the ministry-based DSI family rather than directly comparable to any single branch series.`;
}

function renderTrumpSupplement() {
  renderTrumpSupplementMeta();
  renderTrumpMetricCards();
  renderTrumpChart();
}

function orderedTrumpDirectedCountries() {
  const summaryCountries = state.summary?.countries || [];
  const directedCountries = state.trumpDirected?.countries || [];
  const directedMap = new Map(directedCountries.map((country) => [country.code, country]));
  const ordered = summaryCountries
    .map((country) => directedMap.get(country.code))
    .filter(Boolean);
  const leftovers = directedCountries.filter((country) => !ordered.some((item) => item.code === country.code));
  return [...ordered, ...leftovers];
}

function renderTrumpDirectedMeta() {
  const shell = document.getElementById("trump-directed-shell");
  if (!shell) {
    return;
  }
  if (!state.trumpDirected || !(state.trumpDirected.countries || []).length) {
    shell.hidden = true;
    return;
  }

  shell.hidden = false;
  document.getElementById("trump-directed-coverage-range").textContent =
    `${formatDate(state.trumpDirected.coverage_start)} to ${formatDate(state.trumpDirected.coverage_end)}`;
  document.getElementById("trump-directed-country-count").textContent =
    formatWholeNumber((state.trumpDirected.countries || []).length);
  document.getElementById("trump-directed-candidate-texts").textContent =
    `${formatWholeNumber(state.trumpDirected.accepted_candidate_texts)} / ${formatWholeNumber(state.trumpDirected.candidate_texts_scored)}`;
  document.getElementById("trump-directed-post-country-rows").textContent =
    formatWholeNumber(state.trumpDirected.post_country_rows);
  document.getElementById("trump-directed-mode-note").textContent =
    `Current view: ${trumpSeriesLabel(state.trumpDirectedSeriesMode)}. Tone and geopolitical series stay blank when the selected country is not materially mentioned in the current window.`;
}

function renderTrumpDirectedBoard() {
  const board = document.getElementById("trump-directed-board");
  if (!board || !state.trumpDirected) {
    return;
  }
  board.innerHTML = "";

  orderedTrumpDirectedCountries().forEach((country) => {
    const wdsiCountry = getCountryByCode(country.code) || {};
    const snapshot = getTrumpDirectedBoardSnapshot(country);
    const button = document.createElement("button");
    button.type = "button";
    button.className = `directed-country-card ${country.code === state.trumpDirectedSelectedCode ? "is-active" : ""}`;
    button.style.setProperty("--country-color", wdsiCountry.color || "#1f4e79");
    button.innerHTML = `
      <div class="country-card-heading">
        ${countryFlagMarkup(country.code)}
        <h3>${country.label}</h3>
      </div>
      <div class="directed-country-meta">
        <span>${formatWholeNumber(country.directed_posts_total)} directed posts</span>
        <span>Last mention ${formatDate(country.last_mention_date)}</span>
      </div>
      <div class="directed-country-score ${snapshot.tone.className}">
        ${hasNumericValue(snapshot.value) ? formatScore(snapshot.value) : "--"}
      </div>
      <div class="directed-country-tone">${snapshot.tone.label}</div>
    `;
    button.addEventListener("click", () => {
      void setSelectedTrumpDirectedCountry(country.code);
    });
    board.appendChild(button);
  });
}

function renderTrumpDirectedMetricCards(countryMeta, countryData) {
  const latest = getLatestTrumpDirectedRecord(countryData.records || []);
  if (!latest) {
    return;
  }

  const toneKey = getTrumpDirectedSeriesKey("directed_tone_index");
  const geoKey = getTrumpDirectedSeriesKey("directed_geopolitical_index");
  const attentionKey = state.trumpDirectedSeriesMode === "daily"
    ? "directed_posts"
    : getTrumpDirectedSeriesKey("directed_attention_index");

  const toneValue = latest[toneKey];
  const geoValue = latest[geoKey];
  const attentionValue = latest[attentionKey];
  const tone = trumpToneMeta(toneValue);
  const geo = trumpGeopoliticalMeta(geoValue);
  const attention = directedAttentionMeta(attentionValue);
  const modeLabel = trumpSeriesLabel(state.trumpDirectedSeriesMode);
  const lastMention = formatDate(countryMeta.last_mention_date);

  const toneEl = document.getElementById("trump-directed-tone-latest");
  toneEl.textContent = hasNumericValue(toneValue) ? formatScore(toneValue) : "--";
  toneEl.className = `metric-value ${hasNumericValue(toneValue) ? tone.className : "tone-muted"}`;
  document.getElementById("trump-directed-tone-caption").textContent = hasNumericValue(toneValue)
    ? `${countryMeta.label}'s ${modeLabel} directed tone currently reads ${tone.label}.`
    : `${countryMeta.label} has no material mention in the current ${modeLabel} tone window. Last mention: ${lastMention}.`;

  const geoEl = document.getElementById("trump-directed-geo-latest");
  geoEl.textContent = hasNumericValue(geoValue) ? formatScore(geoValue) : "--";
  geoEl.className = `metric-value ${hasNumericValue(geoValue) ? geo.className : "tone-muted"}`;
  document.getElementById("trump-directed-geo-caption").textContent = hasNumericValue(geoValue)
    ? `${countryMeta.label}'s ${modeLabel} geopolitical stance is currently ${geo.label}.`
    : `${countryMeta.label} has no material mention in the current ${modeLabel} geopolitical window. Last mention: ${lastMention}.`;

  const attentionEl = document.getElementById("trump-directed-attention-latest");
  attentionEl.textContent = state.trumpDirectedSeriesMode === "daily"
    ? formatWholeNumber(attentionValue)
    : formatScore(attentionValue);
  attentionEl.className = `metric-value ${attention.className}`;
  document.getElementById("trump-directed-attention-caption").textContent = state.trumpDirectedSeriesMode === "daily"
    ? `Material directed posts mentioning ${countryMeta.label} on the latest date.`
    : `${modeLabel} log attention intensity is currently ${attention.label}.`;

  document.getElementById("trump-directed-country-name").textContent = countryMeta.label;
  document.getElementById("trump-directed-country-name").className = "metric-value tone-neutral";
  document.getElementById("trump-directed-country-caption").textContent =
    `Total directed posts: ${formatWholeNumber(countryMeta.directed_posts_total)}. Last material mention: ${lastMention}.`;
}

function buildTrumpDirectedChartLayout(countryMeta) {
  return {
    margin: { l: 46, r: 56, t: 36, b: 44 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(255,255,255,0)",
    hovermode: "x unified",
    font: {
      family: '"Source Sans 3", Arial, sans-serif',
      color: "#18212b",
    },
    xaxis: {
      showgrid: true,
      gridcolor: "rgba(24, 33, 43, 0.10)",
      zeroline: false,
      tickfont: { color: "#5c6672" },
    },
    yaxis: {
      title: `${countryMeta.label} directed tone / geopolitics`,
      range: [-3.2, 3.2],
      showgrid: true,
      gridcolor: "rgba(24, 33, 43, 0.10)",
      zeroline: true,
      zerolinecolor: "rgba(24, 33, 43, 0.28)",
      tickfont: { color: "#5c6672" },
    },
    yaxis2: {
      title: state.trumpDirectedSeriesMode === "daily" ? "Directed posts" : "Attention (log mentions)",
      overlaying: "y",
      side: "right",
      rangemode: "tozero",
      showgrid: false,
      tickfont: { color: "#5c6672" },
    },
    legend: {
      orientation: "h",
      yanchor: "bottom",
      y: 1.02,
      xanchor: "left",
      x: 0,
    },
    shapes: [
      {
        type: "rect",
        x0: "2021-01-09",
        x1: "2022-02-13",
        y0: -3.2,
        y1: 3.2,
        line: { width: 0 },
        fillcolor: "rgba(24, 33, 43, 0.06)",
      },
      ...getVisibleTrumpPoliticalEvents(state.trump?.records || []).map((event) => ({
        type: "line",
        x0: event.date,
        x1: event.date,
        y0: -3.2,
        y1: 3.2,
        line: {
          color: "rgba(150, 59, 52, 0.24)",
          width: 1.1,
          dash: "dot",
        },
      })),
    ],
    annotations: [
      {
        x: "2021-07-15",
        y: 2.75,
        xref: "x",
        yref: "y",
        text: "platform gap",
        showarrow: false,
        font: { size: 11, color: "#5c6672" },
      },
    ],
  };
}

function renderTrumpDirectedChart(countryMeta, countryData) {
  const records = countryData.records || [];
  if (!records.length) {
    return;
  }

  const dates = records.map((record) => record.date);
  const toneKey = getTrumpDirectedSeriesKey("directed_tone_index");
  const geoKey = getTrumpDirectedSeriesKey("directed_geopolitical_index");
  const attentionKey = state.trumpDirectedSeriesMode === "daily"
    ? "directed_posts"
    : getTrumpDirectedSeriesKey("directed_attention_index");

  const traces = [
    {
      x: dates,
      y: records.map((record) => record[toneKey]),
      type: "scatter",
      mode: "lines",
      name: "Directed tone",
      line: {
        color: "#963b34",
        width: 2.4,
      },
      hovertemplate: "%{x}<br>Directed tone: %{y:.3f}<extra></extra>",
    },
    {
      x: dates,
      y: records.map((record) => record[geoKey]),
      type: "scatter",
      mode: "lines",
      name: "Directed geopolitical",
      line: {
        color: "#1f4e79",
        width: 2.2,
      },
      hovertemplate: "%{x}<br>Directed geopolitical: %{y:.3f}<extra></extra>",
    },
    state.trumpDirectedSeriesMode === "daily"
      ? {
          x: dates,
          y: records.map((record) => record[attentionKey]),
          type: "bar",
          name: "Directed posts",
          yaxis: "y2",
          marker: {
            color: "rgba(33, 49, 50, 0.38)",
          },
          hovertemplate: "%{x}<br>Directed posts: %{y:.0f}<extra></extra>",
        }
      : {
          x: dates,
          y: records.map((record) => record[attentionKey]),
          type: "scatter",
          mode: "lines",
          name: "Directed attention",
          yaxis: "y2",
          line: {
            color: "#343e49",
            width: 2,
            dash: "dot",
          },
          hovertemplate: "%{x}<br>Directed attention: %{y:.3f}<extra></extra>",
        },
  ];

  Plotly.react("trump-directed-chart", traces, buildTrumpDirectedChartLayout(countryMeta), {
    displayModeBar: true,
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  });

  document.getElementById("trump-directed-chart-caption").textContent =
    `${countryMeta.label} is shown here through Trump's directed tone, directed geopolitical stance, and directed attention. Tone and geopolitical values are blank on windows with no material mention of this country; attention remains available to show whether Trump is talking about it at all.`;
}

async function setSelectedTrumpDirectedCountry(code) {
  state.trumpDirectedSelectedCode = code;
  renderTrumpDirectedBoard();

  const countryMeta = getTrumpDirectedCountryMeta(code);
  if (!countryMeta) {
    return;
  }
  const countryData = await loadTrumpDirectedCountryData(code);
  renderTrumpDirectedMetricCards(countryMeta, countryData);
  renderTrumpDirectedChart(countryMeta, countryData);
}

function bindTrumpDirectedSeriesToggle() {
  const container = document.getElementById("trump-directed-series-toggle");
  if (!container) {
    return;
  }
  container.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-trump-directed-series]");
    if (!button || !state.trumpDirected) {
      return;
    }

    state.trumpDirectedSeriesMode = button.dataset.trumpDirectedSeries;
    container.querySelectorAll("button").forEach((node) => {
      node.classList.toggle("is-active", node === button);
    });

    renderTrumpDirectedMeta();
    if (state.trumpDirectedSelectedCode) {
      void setSelectedTrumpDirectedCountry(state.trumpDirectedSelectedCode);
    }
  });
}

function renderTrumpDirectedSupplement() {
  renderTrumpDirectedMeta();
  renderTrumpDirectedBoard();
}

function bindTrumpSeriesToggle() {
  const container = document.getElementById("trump-series-toggle");
  container.addEventListener("click", (event) => {
    const button = event.target.closest("button[data-trump-series]");
    if (!button || !state.trump) {
      return;
    }

    state.trumpSeriesMode = button.dataset.trumpSeries;
    container.querySelectorAll("button").forEach((node) => {
      node.classList.toggle("is-active", node === button);
    });

    renderTrumpSupplement();
  });
}

async function init() {
  try {
    const [summary, trump, trumpDirected, visitors] = await Promise.all([
      fetchJson(summaryPath),
      fetchJson(trumpPath, trumpDataVersion).catch(() => null),
      fetchJson(trumpDirectedSummaryPath, trumpDataVersion).catch(() => null),
      fetchJson(visitorsPath).catch(() => null),
    ]);
    state.summary = summary;
    state.trump = trump;
    state.trumpDirected = trumpDirected;
    state.visitors = visitors;
    const directedCountries = orderedTrumpDirectedCountries();
    state.trumpDirectedSelectedCode =
      state.trumpDirectedSelectedCode && getTrumpDirectedCountryMeta(state.trumpDirectedSelectedCode)
        ? state.trumpDirectedSelectedCode
        : directedCountries[0]?.code ?? null;

    renderVisitorsCard();
    startVisitorsAutoRefresh();
    bindTrumpSeriesToggle();
    renderTrumpSupplement();
    bindTrumpDirectedSeriesToggle();
    renderTrumpDirectedSupplement();

    if (state.trumpDirectedSelectedCode) {
      await setSelectedTrumpDirectedCountry(state.trumpDirectedSelectedCode);
    }
  } catch (error) {
    console.error(error);
  }
}

window.addEventListener("DOMContentLoaded", init);
