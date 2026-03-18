const state = {
  summary: null,
  events: [],
  selectedCode: null,
  seriesMode: "rolling7",
  cache: new Map(),
};

const summaryPath = "data/summary.json";
const eventsPath = "data/events.json";

function formatDate(dateText) {
  const date = new Date(`${dateText}T00:00:00`);
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
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

function toneMeta(score) {
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

async function fetchJson(path) {
  const response = await fetch(path);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${path}`);
  }
  return response.json();
}

async function loadCountryData(code) {
  if (!state.cache.has(code)) {
    state.cache.set(code, fetchJson(`data/${code}.json`));
  }
  return state.cache.get(code);
}

function renderGlobalMeta() {
  const { overall, generated_at: generatedAt } = state.summary;
  document.getElementById("country-count").textContent = overall.country_count;
  document.getElementById("overall-start").textContent = formatDate(overall.first_date);
  document.getElementById("overall-end").textContent = formatDate(overall.last_date);
  document.getElementById("generated-at").textContent = `Latest build: ${formatBuildTime(generatedAt)}`;
  document.getElementById("footer-build-note").textContent =
    `Covering ${overall.country_count} countries / regions, from ${formatDate(overall.first_date)} to ${formatDate(overall.last_date)}.`;
}

function renderCountryBoard() {
  const board = document.getElementById("country-board");
  board.innerHTML = "";

  state.summary.countries.forEach((country, index) => {
    const tone = toneMeta(country.latest_7d);
    const button = document.createElement("button");
    button.type = "button";
    button.className = `country-card ${country.code === state.selectedCode ? "is-active" : ""}`;
    button.style.setProperty("--country-color", country.color);
    button.style.animationDelay = `${120 + index * 40}ms`;
    button.innerHTML = `
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
    const button = document.createElement("button");
    button.type = "button";
    button.className = `tab-button ${country.code === state.selectedCode ? "is-active" : ""}`;
    button.textContent = `${country.label} - ${country.code}`;
    button.addEventListener("click", () => setSelectedCountry(country.code));
    tabs.appendChild(button);
  });
}

function renderSelectedMetrics(country) {
  const tone = toneMeta(country.latest_7d);
  const latestDelta = country.change_7d ?? 0;
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

  const rolling7El = document.getElementById("selected-rolling7-score");
  rolling7El.textContent = formatScore(country.latest_7d);
  rolling7El.className = `metric-value ${tone.className}`;
  document.getElementById("selected-rolling7-caption").textContent = scoreSentence(
    country.label,
    country.latest_7d,
    "7-day",
  );

  const deltaEl = document.getElementById("selected-change-score");
  deltaEl.textContent = `${latestDelta >= 0 ? "+" : ""}${formatScore(latestDelta)}`;
  deltaEl.className = `metric-value ${deltaTone}`;
  document.getElementById("selected-publication-date").textContent = formatDate(country.latest_publication_date);
  document.getElementById("selected-publication-score").textContent =
    `Latest publication-day raw mean: ${formatScore(country.latest_raw)} - ${scoreDeltaSentence(latestDelta, 7)}`;
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
      title: "Full daily dataset (CSV)",
      meta: "Calendar-day series for all countries, including raw publication-day means plus 7-day and 30-day smoothed values",
      href: "data/wdsi_all_countries.csv",
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
      title: `${country.label} data (CSV)`,
      meta: `${country.code} - ${country.publication_days} publication days`,
      href: country.file_csv,
    },
    {
      title: `${country.label} data (JSON)`,
      meta: "Useful for direct use in web apps or scripts",
      href: country.file_json,
    },
  ]);

  [...masterDownloads, ...countryDownloads].forEach((item) => {
    const row = document.createElement("div");
    row.className = "download-item";
    row.innerHTML = `
      <div>
        <strong>${item.title}</strong>
        <div class="download-meta">${item.meta}</div>
      </div>
      <a class="download-link" href="${item.href}" target="_blank" rel="noreferrer">Open file</a>
    `;
    list.appendChild(row);
  });
}

function renderCsvOnlyDownloadList() {
  const list = document.getElementById("download-list");
  list.innerHTML = "";

  const masterDownloads = [
    {
      title: "Full daily dataset (CSV)",
      meta: "Calendar-day series for all countries, including raw publication-day means plus 7-day and 30-day smoothed values",
      href: "data/wdsi_all_countries.csv",
    },
  ];

  const countryDownloads = state.summary.countries.map((country) => ({
    title: `${country.label} data (CSV)`,
    meta: `${country.code} - ${country.publication_days} publication days`,
    href: country.file_csv,
  }));

  [...masterDownloads, ...countryDownloads].forEach((item) => {
    const row = document.createElement("div");
    row.className = "download-item";
    row.innerHTML = `
      <div>
        <strong>${item.title}</strong>
        <div class="download-meta">${item.meta}</div>
      </div>
      <a class="download-link" href="${item.href}" target="_blank" rel="noreferrer">Open CSV</a>
    `;
    list.appendChild(row);
  });
}

function updateEventChips(countryData) {
  const chips = document.getElementById("event-chips");
  chips.innerHTML = "";

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

function renderChart(country, countryData) {
  const records = countryData.records;
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
        width: showRolling7 ? 3.2 : 1.8,
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
        width: showRolling30 ? 3.2 : 2,
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
      name: "Raw publication-day mean",
      marker: {
        color: "#b85f35",
        size: showRaw ? 8 : 6,
        opacity: showRaw ? 0.88 : 0.22,
      },
      hovertemplate: "%{x}<br>Raw publication-day mean: %{y:.3f}<extra></extra>",
    },
  ];

  Plotly.react("wdsi-chart", traces, buildChartLayout(country, countryData), {
    displayModeBar: true,
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  });

  document.getElementById("chart-caption").textContent =
    `${country.label} is available as a raw publication-day mean together with 7-day and 30-day smoothed series. Current view: ${
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

async function init() {
  try {
    const [summary, events] = await Promise.all([fetchJson(summaryPath), fetchJson(eventsPath)]);
    state.summary = summary;
    state.events = events.events || [];
    state.selectedCode =
      state.selectedCode && getCountryByCode(state.selectedCode)
        ? state.selectedCode
        : state.summary.countries[0]?.code ?? null;

    renderGlobalMeta();
    renderCountryBoard();
    renderCountryTabs();
    renderCsvOnlyDownloadList();
    bindSeriesToggle();

    if (state.selectedCode) {
      await setSelectedCountry(state.selectedCode);
    }
  } catch (error) {
    document.getElementById("chart-caption").textContent =
      "Failed to load page data. Please make sure the data files are accessible and try again.";
    console.error(error);
  }
}

window.addEventListener("DOMContentLoaded", init);
