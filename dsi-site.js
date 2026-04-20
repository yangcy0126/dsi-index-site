const DSI_SITE_STATE = {
  summary: null,
  events: [],
  selectedCode: null,
  indicatorMode: "c1",
  seriesMode: "rolling7",
  cache: new Map(),
  dataVersion: null,
};

const dsiSummaryPath = "data/summary.json";
const dsiEventsPath = "data/events.json";
const dsiAssetVersion = "20260420-dsi-8";

const DSI_INDICATOR_META = {
  c1: {
    shortLabel: "WDSI",
    label: "WDSI",
    longLabel: "War-Related DSI",
    rawKey: "c1_raw",
    filledKey: "c1",
    rolling7Key: "c1_7",
    rolling30Key: "c1_30",
    tonePrefix: "war-related",
  },
  c2: {
    shortLabel: "EDSI",
    label: "EDSI",
    longLabel: "Economic DSI",
    rawKey: "c2_raw",
    filledKey: "c2",
    rolling7Key: "c2_7",
    rolling30Key: "c2_30",
    tonePrefix: "economic",
  },
  c3: {
    shortLabel: "ODSI",
    label: "ODSI",
    longLabel: "Other DSI",
    rawKey: "c3_raw",
    filledKey: "c3",
    rolling7Key: "c3_7",
    rolling30Key: "c3_30",
    tonePrefix: "other diplomatic",
  },
};

function dsiVersionedPath(path, versionOverride = null) {
  const version = versionOverride || dsiAssetVersion;
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}v=${encodeURIComponent(version)}`;
}

async function dsiFetchJson(path, versionOverride = null) {
  const response = await fetch(dsiVersionedPath(path, versionOverride), { cache: "reload" });
  if (!response.ok) {
    throw new Error(`Failed to fetch ${path}`);
  }
  return response.json();
}

function dsiHasNumericValue(value) {
  return value !== null && value !== undefined && !Number.isNaN(Number(value));
}

function dsiFormatDate(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(`${value}T00:00:00`);
  return date.toLocaleDateString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function dsiFormatBuildTime(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  return date.toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function dsiFormatScore(value, digits = 3) {
  if (!dsiHasNumericValue(value)) {
    return "--";
  }
  const rounded = Number(value).toFixed(digits);
  return rounded.replace(/\.?0+$/, "");
}

function dsiEscapeHtml(value) {
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

function dsiToneMeta(score) {
  if (!dsiHasNumericValue(score)) {
    return { label: "no recent signal", className: "tone-muted" };
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

function dsiGetCountry(code) {
  return (DSI_SITE_STATE.summary?.countries || []).find((country) => country.code === code) || null;
}

function dsiGetIndicatorMeta() {
  return DSI_INDICATOR_META[DSI_SITE_STATE.indicatorMode];
}

function dsiGetCountryIndicator(country) {
  const indicator = country?.indicators?.[DSI_SITE_STATE.indicatorMode];
  if (indicator) {
    return indicator;
  }
  return {
    latest_raw: country?.latest_raw ?? null,
    latest_7d: country?.latest_7d ?? null,
    latest_30d: country?.latest_30d ?? null,
    latest_filled: country?.latest_raw ?? null,
    change_7d: country?.change_7d ?? null,
    change_30d: country?.change_30d ?? null,
    current_year_mean: country?.current_year_mean ?? null,
  };
}

function dsiScoreSentence(country, indicatorSummary) {
  const meta = dsiGetIndicatorMeta();
  const score = indicatorSummary.latest_7d;
  const tone = dsiToneMeta(score);
  if (!dsiHasNumericValue(score)) {
    return `${country.label} does not yet have a recent ${meta.shortLabel} signal.`;
  }
  return `${country.label} currently has a 7-day ${meta.shortLabel} of ${dsiFormatScore(score)}, which sits in the ${tone.label} range.`;
}

function dsiDeltaSentence(delta, dayCount) {
  if (!dsiHasNumericValue(delta)) {
    return "Not enough smoothed history yet.";
  }
  if (Math.abs(Number(delta)) < 0.05) {
    return `Little net change over the past ${dayCount} days.`;
  }
  return Number(delta) < 0
    ? `More tense over the past ${dayCount} days.`
    : `More conciliatory over the past ${dayCount} days.`;
}

function dsiReplaceNode(id) {
  const original = document.getElementById(id);
  if (!original || !original.parentNode) {
    return original;
  }
  const clone = original.cloneNode(true);
  original.parentNode.replaceChild(clone, original);
  return clone;
}

function dsiRenderGlobalMeta() {
  const summary = DSI_SITE_STATE.summary;
  if (!summary) {
    return;
  }
  const overall = summary.overall || {};
  const generatedAt = document.getElementById("generated-at");
  const countryCount = document.getElementById("country-count");
  const overallStart = document.getElementById("overall-start");
  const overallEnd = document.getElementById("overall-end");
  const footer = document.getElementById("footer-build-note");

  if (generatedAt) {
    generatedAt.textContent = `Latest build: ${dsiFormatBuildTime(summary.generated_at)}`;
  }
  if (countryCount) {
    countryCount.textContent = overall.country_count ?? "--";
  }
  if (overallStart) {
    overallStart.textContent = dsiFormatDate(overall.first_date);
  }
  if (overallEnd) {
    overallEnd.textContent = dsiFormatDate(overall.last_date);
  }
  if (footer) {
    footer.textContent = `Covering ${overall.country_count} countries / regions across the three DSI branches from ${dsiFormatDate(overall.first_date)} to ${dsiFormatDate(overall.last_date)}.`;
  }
}

function dsiRenderCountryBoard() {
  const board = document.getElementById("country-board");
  if (!board || !DSI_SITE_STATE.summary) {
    return;
  }
  const meta = dsiGetIndicatorMeta();
  board.innerHTML = "";

  DSI_SITE_STATE.summary.countries.forEach((country, index) => {
    const indicatorSummary = dsiGetCountryIndicator(country);
    const tone = dsiToneMeta(indicatorSummary.latest_7d);
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.countryCode = country.code;
    button.className = `country-card ${country.code === DSI_SITE_STATE.selectedCode ? "is-active" : ""}`;
    button.style.setProperty("--country-color", country.color);
    button.style.animationDelay = `${120 + index * 40}ms`;
    button.innerHTML = `
      <h3>${dsiEscapeHtml(country.label)}</h3>
      <div class="country-meta">
        <span>${dsiEscapeHtml(country.code)}</span>
        <span>${meta.shortLabel} | ${dsiFormatDate(country.latest_publication_date)}</span>
      </div>
      <div class="country-score ${tone.className}">${dsiFormatScore(indicatorSummary.latest_7d)}</div>
      <div class="country-tone">${dsiEscapeHtml(tone.label)}</div>
    `;
    board.appendChild(button);
  });
}

function dsiRenderCountryTabs() {
  const tabs = document.getElementById("country-tabs");
  if (!tabs || !DSI_SITE_STATE.summary) {
    return;
  }
  tabs.innerHTML = "";
  DSI_SITE_STATE.summary.countries.forEach((country) => {
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.countryCode = country.code;
    button.className = `tab-button ${country.code === DSI_SITE_STATE.selectedCode ? "is-active" : ""}`;
    button.textContent = `${country.label} - ${country.code}`;
    tabs.appendChild(button);
  });
}

function dsiRenderSelectedMetrics(country) {
  if (!country) {
    return;
  }
  const meta = dsiGetIndicatorMeta();
  const indicatorSummary = dsiGetCountryIndicator(country);
  const latestTone = dsiToneMeta(indicatorSummary.latest_7d);
  const rolling30Tone = dsiToneMeta(indicatorSummary.latest_30d);
  const latestValueEl = document.getElementById("selected-latest-score");
  const rolling30El = document.getElementById("selected-rolling30-score");
  const changeEl = document.getElementById("selected-change-score");
  const changeValue = indicatorSummary.change_30d;
  const changeClass = !dsiHasNumericValue(changeValue)
    ? "tone-muted"
    : Number(changeValue) < -0.05
      ? "tone-negative"
      : Number(changeValue) > 0.05
        ? "tone-positive"
        : "tone-neutral";

  document.getElementById("selected-country-name").textContent = country.label;
  document.getElementById("selected-current-label").textContent = `${meta.shortLabel} 7-day`;
  document.getElementById("selected-rolling30-label").textContent = `${meta.shortLabel} 30-day`;
  document.getElementById("selected-change-label").textContent = `${meta.shortLabel} 30-day change`;

  latestValueEl.textContent = dsiFormatScore(indicatorSummary.latest_7d);
  latestValueEl.className = `metric-value ${latestTone.className}`;
  document.getElementById("selected-score-caption").textContent = dsiScoreSentence(country, indicatorSummary);

  rolling30El.textContent = dsiFormatScore(indicatorSummary.latest_30d);
  rolling30El.className = `metric-value ${rolling30Tone.className}`;
  document.getElementById("selected-rolling30-caption").textContent =
    `${country.label}'s 30-day ${meta.shortLabel} is ${dsiFormatScore(indicatorSummary.latest_30d)}.`;

  changeEl.textContent = dsiHasNumericValue(changeValue)
    ? `${Number(changeValue) >= 0 ? "+" : ""}${dsiFormatScore(changeValue)}`
    : "--";
  changeEl.className = `metric-value ${changeClass}`;

  document.getElementById("selected-publication-date").textContent = dsiFormatDate(country.latest_publication_date);
  document.getElementById("selected-publication-score").textContent =
    `${country.latest_title || "Latest official publication"} | raw ${meta.shortLabel}: ${dsiFormatScore(indicatorSummary.latest_raw)} | ${dsiDeltaSentence(changeValue, 30)}`;
  document.getElementById("selected-coverage").textContent =
    `${dsiFormatDate(country.start_date)} to ${dsiFormatDate(country.latest_date)}`;
  document.getElementById("selected-publication-days").textContent =
    `${country.publication_days} publication days | ${country.calendar_days} calendar days | ${meta.shortLabel}`;
}

function dsiRenderDownloadList() {
  const list = document.getElementById("download-list");
  if (!list || !DSI_SITE_STATE.summary) {
    return;
  }
  const downloads = DSI_SITE_STATE.summary.downloads || {};
  const items = [
    {
      title: "Full DSI daily dataset",
      meta: "Combined cross-country workbook covering all three DSI branches and variable definitions.",
      href: downloads.dsi_xlsx || "data/dsi_all_countries.xlsx",
    },
    {
      title: "WDSI cross-country panel",
      meta: "Cross-country workbook for the war-related DSI branch only.",
      href: downloads.wdsi_xlsx || "data/wdsi_all_countries.xlsx",
    },
    {
      title: "EDSI cross-country panel",
      meta: "Cross-country workbook for the economic DSI branch only.",
      href: downloads.edsi_xlsx || "data/edsi_all_countries.xlsx",
    },
    {
      title: "ODSI cross-country panel",
      meta: "Cross-country workbook for the other-diplomatic DSI branch only.",
      href: downloads.odsi_xlsx || "data/odsi_all_countries.xlsx",
    },
    {
      title: "Site summary (JSON)",
      meta: "Country list, three-indicator latest values, coverage windows, and download paths.",
      href: downloads.summary_json || "data/summary.json",
    },
    ...DSI_SITE_STATE.summary.countries.map((country) => ({
      title: `${country.label} data`,
      meta: `${country.code} | ${country.publication_days} publication days | workbook contains all three DSI branches`,
      href: country.file_xlsx || country.file_csv,
    })),
  ];

  list.innerHTML = "";
  items.forEach((item) => {
    const row = document.createElement("div");
    row.className = `download-item ${item.href === "data/dsi_all_countries.xlsx" ? "download-item-wide" : ""}`.trim();
    row.innerHTML = `
      <div>
        <strong>${dsiEscapeHtml(item.title)}</strong>
        <div class="download-meta">${dsiEscapeHtml(item.meta)}</div>
      </div>
      <a class="download-link" href="${item.href}" target="_blank" rel="noreferrer">Download</a>
    `;
    list.appendChild(row);
  });
}

function dsiBuildChartLayout(country, records) {
  const meta = dsiGetIndicatorMeta();
  const start = records[0].date;
  const end = records[records.length - 1].date;
  const filteredEvents = (DSI_SITE_STATE.events || []).filter((event) => event.date >= start && event.date <= end);

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
      title: meta.shortLabel,
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

function dsiUpdateEventChips(records) {
  const chips = document.getElementById("event-chips");
  if (!chips) {
    return;
  }
  chips.innerHTML = "";
  if (!records.length) {
    return;
  }
  const start = records[0].date;
  const end = records[records.length - 1].date;
  const visibleEvents = (DSI_SITE_STATE.events || []).filter((event) => event.date >= start && event.date <= end);
  visibleEvents.forEach((event) => {
    const chip = document.createElement("span");
    chip.className = "event-chip";
    chip.textContent = `${event.date} - ${event.title_en}`;
    chips.appendChild(chip);
  });
}

function dsiRenderChart(country, countryData) {
  const records = countryData.records || [];
  if (!records.length || typeof Plotly === "undefined") {
    return;
  }
  const meta = dsiGetIndicatorMeta();
  const rollingDates = records.map((record) => record.date);
  const rolling7Values = records.map((record) => record[meta.rolling7Key]);
  const rolling30Values = records.map((record) => record[meta.rolling30Key]);
  const rawRecords = records.filter((record) => record.publication && record[meta.rawKey] !== null && record[meta.rawKey] !== undefined);
  const rawDates = rawRecords.map((record) => record.date);
  const rawValues = rawRecords.map((record) => record[meta.rawKey]);
  const showRolling7 = DSI_SITE_STATE.seriesMode === "rolling7";
  const showRolling30 = DSI_SITE_STATE.seriesMode === "rolling30";
  const showRaw = DSI_SITE_STATE.seriesMode === "raw";

  const traces = [
    {
      x: rollingDates,
      y: rolling7Values,
      type: "scatter",
      mode: "lines",
      name: `7-day smoothed ${meta.shortLabel}`,
      line: {
        color: country.color,
        width: showRolling7 ? 2.6 : 1.4,
      },
      opacity: showRolling7 ? 1 : showRaw ? 0.42 : 0.3,
      hovertemplate: `%{x}<br>7-day ${meta.shortLabel}: %{y:.3f}<extra></extra>`,
    },
    {
      x: rollingDates,
      y: rolling30Values,
      type: "scatter",
      mode: "lines",
      name: `30-day smoothed ${meta.shortLabel}`,
      line: {
        color: country.color,
        width: showRolling30 ? 2.6 : 1.5,
        dash: "longdash",
      },
      opacity: showRolling30 ? 0.95 : showRaw ? 0.38 : 0.32,
      hovertemplate: `%{x}<br>30-day ${meta.shortLabel}: %{y:.3f}<extra></extra>`,
    },
    {
      x: rawDates,
      y: rawValues,
      type: "scatter",
      mode: "markers",
      name: `${meta.shortLabel} raw publication-day score`,
      marker: {
        color: "#b85f35",
        size: showRaw ? 8 : 6,
        opacity: showRaw ? 0.88 : 0.22,
      },
      hovertemplate: `%{x}<br>${meta.shortLabel} raw: %{y:.0f}<extra></extra>`,
    },
  ];

  Plotly.react("wdsi-chart", traces, dsiBuildChartLayout(country, records), {
    displayModeBar: true,
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  });

  document.getElementById("chart-caption").textContent =
    `${country.label} is shown here through ${meta.longLabel}. Current view: ${
      showRolling7 ? "7-day smoothed trend" : showRolling30 ? "30-day smoothed trend" : "publication-day raw moves"
    }. This site presents the three DSI branches on equal footing.`;

  dsiUpdateEventChips(records);
}

async function dsiLoadCountryData(code) {
  const version = DSI_SITE_STATE.dataVersion || dsiAssetVersion;
  const cacheKey = `${code}::${version}`;
  if (!DSI_SITE_STATE.cache.has(cacheKey)) {
    DSI_SITE_STATE.cache.set(cacheKey, dsiFetchJson(`data/${code}.json`, version));
  }
  return DSI_SITE_STATE.cache.get(cacheKey);
}

async function dsiSetSelectedCountry(code) {
  DSI_SITE_STATE.selectedCode = code;
  dsiRenderCountryBoard();
  dsiRenderCountryTabs();
  const country = dsiGetCountry(code);
  if (!country) {
    return;
  }
  dsiRenderSelectedMetrics(country);
  const countryData = await dsiLoadCountryData(code);
  dsiRenderChart(country, countryData);
}

function dsiBindCountrySelectors() {
  const board = document.getElementById("country-board");
  const tabs = document.getElementById("country-tabs");
  const handler = (event) => {
    const button = event.target.closest("button[data-country-code]");
    if (!button) {
      return;
    }
    void dsiSetSelectedCountry(button.dataset.countryCode);
  };
  board?.addEventListener("click", handler);
  tabs?.addEventListener("click", handler);
}

function dsiSyncIndicatorToggles() {
  document.querySelectorAll("[data-indicator-toggle]").forEach((container) => {
    container.querySelectorAll("button[data-indicator]").forEach((node) => {
      node.classList.toggle("is-active", node.dataset.indicator === DSI_SITE_STATE.indicatorMode);
    });
  });
}

function dsiBindIndicatorToggle() {
  const containers = Array.from(document.querySelectorAll("[data-indicator-toggle]"));
  if (!containers.length) {
    return;
  }
  containers.forEach((container) => {
    container.addEventListener("click", async (event) => {
      const button = event.target.closest("button[data-indicator]");
      if (!button) {
        return;
      }
      DSI_SITE_STATE.indicatorMode = button.dataset.indicator;
      dsiSyncIndicatorToggles();
      if (DSI_SITE_STATE.selectedCode) {
        await dsiSetSelectedCountry(DSI_SITE_STATE.selectedCode);
      }
    });
  });
}

function dsiBindSeriesToggle() {
  const container = dsiReplaceNode("series-toggle");
  if (!container) {
    return;
  }
  container.addEventListener("click", async (event) => {
    const button = event.target.closest("button[data-series]");
    if (!button) {
      return;
    }
    DSI_SITE_STATE.seriesMode = button.dataset.series;
    container.querySelectorAll("button").forEach((node) => {
      node.classList.toggle("is-active", node === button);
    });
    if (DSI_SITE_STATE.selectedCode) {
      await dsiSetSelectedCountry(DSI_SITE_STATE.selectedCode);
    }
  });
}

async function initDSISite() {
  try {
    const [summary, events] = await Promise.all([
      dsiFetchJson(dsiSummaryPath),
      dsiFetchJson(dsiEventsPath).catch(() => ({ events: [] })),
    ]);
    DSI_SITE_STATE.summary = summary;
    DSI_SITE_STATE.events = events.events || [];
    DSI_SITE_STATE.dataVersion = summary.generated_at || dsiAssetVersion;
    DSI_SITE_STATE.selectedCode = summary.countries?.[0]?.code ?? null;

    dsiRenderGlobalMeta();
    dsiRenderDownloadList();
    dsiBindCountrySelectors();
    dsiBindIndicatorToggle();
    dsiBindSeriesToggle();
    dsiSyncIndicatorToggles();

    if (DSI_SITE_STATE.selectedCode) {
      await dsiSetSelectedCountry(DSI_SITE_STATE.selectedCode);
    }
  } catch (error) {
    const caption = document.getElementById("chart-caption");
    if (caption) {
      caption.textContent = "Failed to load DSI site data. Please verify that the rebuilt site files are available.";
    }
    console.error(error);
  }
}

window.addEventListener("DOMContentLoaded", initDSISite);
