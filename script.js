const state = {
  summary: null,
  events: [],
  trump: null,
  trumpDirected: null,
  visitors: null,
  selectedCode: null,
  seriesMode: "rolling7",
  trumpSeriesMode: "7d",
  trumpDirectedSeriesMode: "30d",
  trumpDirectedSelectedCode: null,
  cache: new Map(),
  trumpDirectedCache: new Map(),
};

const summaryPath = "data/summary.json";
const eventsPath = "data/events.json";
const trumpPath = "data/trump_indices.json";
const trumpDirectedSummaryPath = "data/trump_directed_summary.json";
const trumpDirectedBasePath = "data/trump_directed";
const visitorsPath = "data/visitor_stats.json";
const assetVersion = "20260414-dsi-7";
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

function formatBuildTime(dateText) {
  const date = new Date(dateText);
  return date.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
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

function toneMeta(score) {
  if (!hasNumericValue(score)) {
    return { label: "placeholder", className: "tone-muted" };
  }
  if (score <= -1.5) {
    return { label: "markedly tense", className: "tone-negative" };
  }
  if (score <= -0.5) {
    return { label: "somewhat tense", className: "tone-negative" };
  }
  if (score < 0.5) {
    return { label: "near neutral", className: "tone-neutral" };
  }
  if (score < 1.5) {
    return { label: "somewhat conciliatory", className: "tone-positive" };
  }
  return { label: "markedly conciliatory", className: "tone-positive" };
}

function scoreSentence(label, score, windowLabel) {
  if (!hasNumericValue(score)) {
    return `${label} is currently reserved as a top-15 GDP placeholder while source onboarding is in progress.`;
  }
  const tone = toneMeta(score);
  return `${label} currently has a ${windowLabel} smoothed value of ${formatScore(score)}, placing it in the ${tone.label} range.`;
}

function scoreDeltaSentence(delta, dayCount = 30) {
  if (delta === null || delta === undefined || Number.isNaN(delta)) {
    return "Not enough observations yet.";
  }
  if (Math.abs(delta) < 0.05) {
    return `Little net change over the past ${dayCount} days.`;
  }
  return delta < 0
    ? `More tense over the past ${dayCount} days.`
    : `More conciliatory over the past ${dayCount} days.`;
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

async function loadCountryData(code) {
  if (!state.cache.has(code)) {
    state.cache.set(code, fetchJson(`data/${code}.json`));
  }
  return state.cache.get(code);
}

async function loadTrumpDirectedCountryData(code) {
  if (!state.trumpDirectedCache.has(code)) {
    state.trumpDirectedCache.set(code, fetchJson(`${trumpDirectedBasePath}/${code}.json`));
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

  const topCountries = snapshot.top_countries || [];
  list.innerHTML = topCountries.length
    ? topCountries.map((country) => `
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

function renderGlobalMeta() {
  const { overall, generated_at: generatedAt } = state.summary;
  const liveCount = overall.live_country_count ?? overall.country_count;
  const placeholderCount = overall.placeholder_count ?? 0;
  document.getElementById("country-count").textContent =
    placeholderCount > 0 ? `${overall.country_count} total` : overall.country_count;
  document.getElementById("overall-start").textContent = formatDate(overall.first_date);
  document.getElementById("overall-end").textContent = formatDate(overall.last_date);
  document.getElementById("generated-at").textContent = `Latest build: ${formatBuildTime(generatedAt)}`;
  document.getElementById("footer-build-note").textContent = placeholderCount > 0
    ? `Covering ${overall.country_count} countries / regions, with ${liveCount} live series and ${placeholderCount} placeholders, from ${formatDate(overall.first_date)} to ${formatDate(overall.last_date)}.`
    : `Covering ${overall.country_count} countries / regions, from ${formatDate(overall.first_date)} to ${formatDate(overall.last_date)}.`;
}

function renderCountryBoard() {
  const board = document.getElementById("country-board");
  board.innerHTML = "";

  state.summary.countries.forEach((country, index) => {
    const isPlaceholder = isPlaceholderCountry(country);
    const tone = toneMeta(country.latest_7d);
    const button = document.createElement("button");
    button.type = "button";
    button.className = `country-card ${country.code === state.selectedCode ? "is-active" : ""} ${isPlaceholder ? "is-placeholder" : ""}`;
    button.style.setProperty("--country-color", country.color);
    button.style.animationDelay = `${120 + index * 40}ms`;
    button.innerHTML = isPlaceholder ? `
      <h3>${country.label}</h3>
      <div class="country-meta">
        <span>${country.code}</span>
        <span>GDP top-15 placeholder</span>
      </div>
      <div class="country-score tone-muted">Placeholder</div>
      <div class="country-tone">Source onboarding pending</div>
    ` : `
      <h3>${country.label}</h3>
      <div class="country-meta">
        <span>${country.code}</span>
        <span>Latest publication ${formatDate(country.latest_publication_date)}</span>
      </div>
      <div class="country-score ${tone.className}">${formatScore(country.latest_7d)}</div>
      <div class="country-tone">${tone.label}</div>
    `;
    button.addEventListener("click", () => setSelectedCountry(country.code));
    board.appendChild(button);
  });
}

function renderCountryTabs() {
  const tabs = document.getElementById("country-tabs");
  tabs.innerHTML = "";

  state.summary.countries.forEach((country) => {
    const isPlaceholder = isPlaceholderCountry(country);
    const button = document.createElement("button");
    button.type = "button";
    button.className = `tab-button ${country.code === state.selectedCode ? "is-active" : ""} ${isPlaceholder ? "is-placeholder" : ""}`;
    button.textContent = isPlaceholder
      ? `${country.label} - ${country.code} placeholder`
      : `${country.label} - ${country.code}`;
    button.addEventListener("click", () => setSelectedCountry(country.code));
    tabs.appendChild(button);
  });
}

function renderSelectedMetrics(country) {
  if (isPlaceholderCountry(country)) {
    document.getElementById("selected-country-name").textContent = country.label;
    document.getElementById("selected-latest-score").textContent = "--";
    document.getElementById("selected-latest-score").className = "metric-value tone-muted";
    document.getElementById("selected-score-caption").textContent =
      `${country.label} is currently reserved as a GDP top-15 placeholder.`;
    document.getElementById("selected-rolling30-score").textContent = "--";
    document.getElementById("selected-rolling30-score").className = "metric-value tone-muted";
    document.getElementById("selected-rolling30-caption").textContent =
      "The 30-day smoothed series will appear here after source onboarding and validation.";
    document.getElementById("selected-change-score").textContent = "--";
    document.getElementById("selected-change-score").className = "metric-value tone-muted";
    document.getElementById("selected-publication-date").textContent = "Pending";
    document.getElementById("selected-publication-score").textContent =
      country.placeholder_note || "No DSI series has been published yet for this slot.";
    document.getElementById("selected-coverage").textContent = "Reserved slot";
    document.getElementById("selected-publication-days").textContent = "Official-source pipeline pending";
    return;
  }

  const tone = toneMeta(country.latest_7d);
  const rolling30Tone = toneMeta(country.latest_30d);
  const latestDelta = country.change_30d ?? 0;
  const deltaTone =
    latestDelta < -0.05 ? "tone-negative" : latestDelta > 0.05 ? "tone-positive" : "tone-neutral";

  document.getElementById("selected-country-name").textContent = country.label;
  document.getElementById("selected-latest-score").textContent = formatScore(country.latest_7d);
  document.getElementById("selected-latest-score").className = `metric-value ${tone.className}`;
  document.getElementById("selected-score-caption").textContent = scoreSentence(
    country.label,
    country.latest_7d,
    "7-day",
  );

  const rolling30El = document.getElementById("selected-rolling30-score");
  rolling30El.textContent = formatScore(country.latest_30d);
  rolling30El.className = `metric-value ${rolling30Tone.className}`;
  document.getElementById("selected-rolling30-caption").textContent = scoreSentence(
    country.label,
    country.latest_30d,
    "30-day",
  );

  const deltaEl = document.getElementById("selected-change-score");
  deltaEl.textContent = `${latestDelta >= 0 ? "+" : ""}${formatScore(latestDelta)}`;
  deltaEl.className = `metric-value ${deltaTone}`;
  document.getElementById("selected-publication-date").textContent = formatDate(country.latest_publication_date);
  document.getElementById("selected-publication-score").textContent =
    `Latest publication-day raw score: ${formatScore(country.latest_raw)} - ${scoreDeltaSentence(latestDelta, 30)}`;
  document.getElementById("selected-coverage").textContent =
    `${formatDate(country.start_date)} to ${formatDate(country.latest_date)}`;
  document.getElementById("selected-publication-days").textContent =
    `${country.publication_days} publication days - ${country.calendar_days} calendar days`;
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

function renderCsvOnlyDownloadList() {
  const list = document.getElementById("download-list");
  list.innerHTML = "";

  const masterDownloads = [
    {
      title: "Full daily dataset",
      meta: "Excel workbook with the full calendar-day panel and a variable-definitions sheet",
      href: "data/wdsi_all_countries.xlsx",
    },
  ];

  const countryDownloads = state.summary.countries.map((country) => ({
    title: `${country.label} data`,
    meta: isPlaceholderCountry(country)
      ? `${country.code} - placeholder slot, CSV pending`
      : `${country.code} - ${country.publication_days} publication days`,
    href: isPlaceholderCountry(country) ? null : country.file_xlsx || country.file_csv,
  }));

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

  const toneEl = document.getElementById("trump-tone-latest");
  toneEl.textContent = formatScore(toneValue);
  toneEl.className = `metric-value ${tone.className}`;
  document.getElementById("trump-tone-caption").textContent =
    `${modeLabel} average across all authored Trump posts. Latest reading suggests ${tone.label} rhetoric.`;

  const geoEl = document.getElementById("trump-geo-latest");
  geoEl.textContent = formatScore(geoValue);
  geoEl.className = `metric-value ${geo.className}`;
  document.getElementById("trump-geo-caption").textContent =
    `${modeLabel} mean for China, tariffs, war, NATO, migration, and other geopolitical posts. Latest stance is ${geo.label}.`;

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
      family: '"Space Grotesk", sans-serif',
      color: "#213132",
    },
    xaxis: {
      showgrid: true,
      gridcolor: "rgba(20, 38, 40, 0.08)",
      zeroline: false,
      tickfont: { color: "#5b6968" },
    },
    yaxis: {
      title: "Trump index",
      range: [-3.2, 3.2],
      showgrid: true,
      gridcolor: "rgba(20, 38, 40, 0.08)",
      zeroline: true,
      zerolinecolor: "rgba(20, 38, 40, 0.18)",
      tickfont: { color: "#5b6968" },
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
        fillcolor: "rgba(20, 38, 40, 0.06)",
      },
      ...politicalEvents.map((event) => ({
        type: "line",
        x0: event.date,
        x1: event.date,
        y0: -3.2,
        y1: 3.2,
        line: {
          color: "rgba(184, 95, 53, 0.22)",
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
        font: { size: 11, color: "#5b6968" },
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
        bordercolor: "rgba(184, 95, 53, 0.18)",
        borderwidth: 1,
        borderpad: 4,
        font: { size: 10, color: "#7b4b33" },
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
        color: "#b85f35",
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
        color: "#0f6c74",
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
        color: "#213132",
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
    button.style.setProperty("--country-color", wdsiCountry.color || "#0f6c74");
    button.innerHTML = `
      <h3>${country.label}</h3>
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
      family: '"Space Grotesk", sans-serif',
      color: "#213132",
    },
    xaxis: {
      showgrid: true,
      gridcolor: "rgba(20, 38, 40, 0.08)",
      zeroline: false,
      tickfont: { color: "#5b6968" },
    },
    yaxis: {
      title: `${countryMeta.label} directed tone / geopolitics`,
      range: [-3.2, 3.2],
      showgrid: true,
      gridcolor: "rgba(20, 38, 40, 0.08)",
      zeroline: true,
      zerolinecolor: "rgba(20, 38, 40, 0.18)",
      tickfont: { color: "#5b6968" },
    },
    yaxis2: {
      title: state.trumpDirectedSeriesMode === "daily" ? "Directed posts" : "Attention (log mentions)",
      overlaying: "y",
      side: "right",
      rangemode: "tozero",
      showgrid: false,
      tickfont: { color: "#5b6968" },
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
        fillcolor: "rgba(20, 38, 40, 0.06)",
      },
      ...getVisibleTrumpPoliticalEvents(state.trump?.records || []).map((event) => ({
        type: "line",
        x0: event.date,
        x1: event.date,
        y0: -3.2,
        y1: 3.2,
        line: {
          color: "rgba(184, 95, 53, 0.22)",
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
        font: { size: 11, color: "#5b6968" },
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
        color: "#b85f35",
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
        color: "#0f6c74",
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
            color: "#213132",
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

function updateEventChips(countryData) {
  const chips = document.getElementById("event-chips");
  chips.innerHTML = "";
  if (!countryData.records || !countryData.records.length) {
    return;
  }

  const minDate = countryData.records[0].date;
  const maxDate = countryData.records[countryData.records.length - 1].date;
  const visibleEvents = state.events.filter((event) => event.date >= minDate && event.date <= maxDate);

  visibleEvents.forEach((event) => {
    const chip = document.createElement("span");
    chip.className = "event-chip";
    chip.textContent = `${event.date} - ${event.title_en}`;
    chips.appendChild(chip);
  });
}

function buildChartLayout(country, countryData) {
  const start = countryData.records[0].date;
  const end = countryData.records[countryData.records.length - 1].date;
  const filteredEvents = state.events.filter((event) => event.date >= start && event.date <= end);

  return {
    margin: { l: 46, r: 24, t: 36, b: 44 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(255,255,255,0)",
    hovermode: "x unified",
    font: {
      family: '"Space Grotesk", sans-serif',
      color: "#213132",
    },
    xaxis: {
      showgrid: true,
      gridcolor: "rgba(20, 38, 40, 0.08)",
      zeroline: false,
      tickfont: { color: "#5b6968" },
    },
    yaxis: {
      title: "WDSI",
      range: [-3.2, 3.2],
      showgrid: true,
      gridcolor: "rgba(20, 38, 40, 0.08)",
      zeroline: true,
      zerolinecolor: "rgba(20, 38, 40, 0.18)",
      tickfont: { color: "#5b6968" },
    },
    legend: {
      orientation: "h",
      yanchor: "bottom",
      y: 1.02,
      xanchor: "left",
      x: 0,
    },
    shapes: filteredEvents.map((event) => ({
      type: "line",
      x0: event.date,
      x1: event.date,
      y0: -3.2,
      y1: 3.2,
      line: {
        color: "rgba(184, 95, 53, 0.22)",
        width: 1.2,
        dash: "dot",
      },
    })),
  };
}

function buildPlaceholderChartLayout(country) {
  return {
    margin: { l: 24, r: 24, t: 24, b: 24 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(255,255,255,0)",
    font: {
      family: '"Space Grotesk", sans-serif',
      color: "#213132",
    },
    xaxis: { visible: false },
    yaxis: { visible: false },
    annotations: [
      {
        x: 0.5,
        y: 0.58,
        xref: "paper",
        yref: "paper",
        showarrow: false,
        text: `${country.label} is reserved as a top-15 GDP placeholder`,
        font: { size: 18, color: "#213132" },
      },
      {
        x: 0.5,
        y: 0.42,
        xref: "paper",
        yref: "paper",
        showarrow: false,
        text: country.placeholder_note || "The time series will appear after official-source onboarding and validation.",
        font: { size: 13, color: "#5b6968" },
        align: "center",
      },
    ],
  };
}

function renderChart(country, countryData) {
  const records = countryData.records;
  if (!records.length) {
    Plotly.react("wdsi-chart", [], buildPlaceholderChartLayout(country), {
      displayModeBar: false,
      responsive: true,
      displaylogo: false,
    });
    document.getElementById("chart-caption").textContent =
      `${country.label} is currently shown as a GDP top-15 placeholder. The chart will populate once DSI collection and validation are complete.`;
    updateEventChips(countryData);
    return;
  }
  const rollingDates = records.map((record) => record.date);
  const rolling7Values = records.map((record) => record.rolling7);
  const rolling30Values = records.map((record) => record.rolling30);
  const rawRecords = records.filter((record) => record.publication && record.raw !== null);
  const rawDates = rawRecords.map((record) => record.date);
  const rawValues = rawRecords.map((record) => record.raw);
  const showRolling7 = state.seriesMode === "rolling7";
  const showRolling30 = state.seriesMode === "rolling30";
  const showRaw = state.seriesMode === "raw";

  const traces = [
    {
      x: rollingDates,
      y: rolling7Values,
      type: "scatter",
      mode: "lines",
      name: "7-day smoothed WDSI",
      line: {
        color: country.color,
        width: showRolling7 ? 2.6 : 1.4,
        dash: "solid",
      },
      opacity: showRolling7 ? 1 : showRaw ? 0.42 : 0.3,
      hovertemplate: "%{x}<br>7-day smoothed value: %{y:.3f}<extra></extra>",
    },
    {
      x: rollingDates,
      y: rolling30Values,
      type: "scatter",
      mode: "lines",
      name: "30-day smoothed WDSI",
      line: {
        color: country.color,
        width: showRolling30 ? 2.6 : 1.5,
        dash: "longdash",
      },
      opacity: showRolling30 ? 0.95 : showRaw ? 0.38 : 0.32,
      hovertemplate: "%{x}<br>30-day smoothed value: %{y:.3f}<extra></extra>",
    },
    {
      x: rawDates,
      y: rawValues,
      type: "scatter",
      mode: "markers",
      name: "Raw publication-day score",
      marker: {
        color: "#b85f35",
        size: showRaw ? 8 : 6,
        opacity: showRaw ? 0.88 : 0.22,
      },
      hovertemplate: "%{x}<br>Raw publication-day score: %{y:.0f}<extra></extra>",
    },
  ];

  Plotly.react("wdsi-chart", traces, buildChartLayout(country, countryData), {
    displayModeBar: true,
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  });

  document.getElementById("chart-caption").textContent =
    `${country.label} is available as a raw publication-day score together with 7-day and 30-day smoothed series. Current view: ${
      showRolling7 ? "7-day smoothed trend" : showRolling30 ? "30-day smoothed trend" : "discrete publication-day moves"
    }.`;

  updateEventChips(countryData);
}

async function setSelectedCountry(code) {
  state.selectedCode = code;
  renderCountryBoard();
  renderCountryTabs();

  const country = getCountryByCode(code);
  renderSelectedMetrics(country);
  const countryData = await loadCountryData(code);
  renderChart(country, countryData);
}

function bindSeriesToggle() {
  const container = document.getElementById("series-toggle");
  container.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-series]");
    if (!button) {
      return;
    }

    state.seriesMode = button.dataset.series;
    container.querySelectorAll("button").forEach((node) => {
      node.classList.toggle("is-active", node === button);
    });

    const country = getCountryByCode(state.selectedCode);
    const countryData = await loadCountryData(state.selectedCode);
    renderChart(country, countryData);
  });
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
    const [summary, events, trump, trumpDirected, visitors] = await Promise.all([
      fetchJson(summaryPath),
      fetchJson(eventsPath),
      fetchJson(trumpPath).catch(() => null),
      fetchJson(trumpDirectedSummaryPath).catch(() => null),
      fetchJson(visitorsPath).catch(() => null),
    ]);
    state.summary = summary;
    state.events = events.events || [];
    state.trump = trump;
    state.trumpDirected = trumpDirected;
    state.visitors = visitors;
    state.selectedCode =
      state.selectedCode && getCountryByCode(state.selectedCode)
        ? state.selectedCode
        : state.summary.countries[0]?.code ?? null;
    const directedCountries = orderedTrumpDirectedCountries();
    state.trumpDirectedSelectedCode =
      state.trumpDirectedSelectedCode && getTrumpDirectedCountryMeta(state.trumpDirectedSelectedCode)
        ? state.trumpDirectedSelectedCode
        : directedCountries[0]?.code ?? null;

    renderGlobalMeta();
    renderVisitorsCard();
    startVisitorsAutoRefresh();
    renderCountryBoard();
    renderCountryTabs();
    renderCsvOnlyDownloadList();
    bindSeriesToggle();
    bindTrumpSeriesToggle();
    renderTrumpSupplement();
    bindTrumpDirectedSeriesToggle();
    renderTrumpDirectedSupplement();

    if (state.selectedCode) {
      await setSelectedCountry(state.selectedCode);
    }
    if (state.trumpDirectedSelectedCode) {
      await setSelectedTrumpDirectedCountry(state.trumpDirectedSelectedCode);
    }
  } catch (error) {
    document.getElementById("chart-caption").textContent =
      "Failed to load page data. Please make sure the data files are accessible and try again.";
    console.error(error);
  }
}

window.addEventListener("DOMContentLoaded", init);
