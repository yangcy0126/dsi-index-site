const state = {
  summary: null,
  events: [],
  selectedCode: null,
  seriesMode: "rolling",
  cache: new Map(),
};

const summaryPath = "data/summary.json";
const eventsPath = "data/events.json";

function formatDate(dateText) {
  const date = new Date(`${dateText}T00:00:00`);
  return date.toLocaleDateString("zh-CN", {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function formatBuildTime(dateText) {
  const date = new Date(dateText);
  return date.toLocaleString("zh-CN", {
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
    return { label: "明显偏紧张", className: "tone-negative" };
  }
  if (score <= -0.5) {
    return { label: "偏紧张", className: "tone-negative" };
  }
  if (score < 0.5) {
    return { label: "中性附近", className: "tone-neutral" };
  }
  if (score < 1.5) {
    return { label: "偏缓和", className: "tone-positive" };
  }
  return { label: "明显偏缓和", className: "tone-positive" };
}

function scoreSentence(country) {
  const tone = toneMeta(country.latest_7d);
  return `${country.label_zh} 当前 7 日平滑值为 ${formatScore(country.latest_7d)}，属于${tone.label}区间。`;
}

function scoreDeltaSentence(delta) {
  if (delta === null || delta === undefined || Number.isNaN(delta)) {
    return "样本长度不足，暂不计算。";
  }
  if (Math.abs(delta) < 0.05) {
    return "近 30 天整体变化很小。";
  }
  return delta < 0 ? "近 30 天更偏向紧张。" : "近 30 天更偏向缓和。";
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
  document.getElementById("generated-at").textContent = `最新构建：${formatBuildTime(generatedAt)}`;
  document.getElementById("footer-build-note").textContent =
    `全站覆盖 ${overall.country_count} 个国家 / 地区，最早观测 ${formatDate(overall.first_date)}，最新观测 ${formatDate(overall.last_date)}。`;
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
      <h3>${country.label_zh}</h3>
      <div class="country-meta">
        <span>${country.label}</span>
        <span>最新发文 ${formatDate(country.latest_publication_date)}</span>
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
    button.textContent = `${country.label_zh} · ${country.code}`;
    button.addEventListener("click", () => setSelectedCountry(country.code));
    tabs.appendChild(button);
  });
}

function renderSelectedMetrics(country) {
  const tone = toneMeta(country.latest_7d);
  const latestDelta = country.change_30d ?? 0;
  const deltaTone =
    latestDelta < -0.05 ? "tone-negative" : latestDelta > 0.05 ? "tone-positive" : "tone-neutral";

  document.getElementById("selected-country-name").textContent = `${country.label_zh} · ${country.label}`;
  document.getElementById("selected-latest-score").textContent = formatScore(country.latest_7d);
  document.getElementById("selected-latest-score").className = `metric-value ${tone.className}`;
  document.getElementById("selected-score-caption").textContent = scoreSentence(country);

  const deltaEl = document.getElementById("selected-change-score");
  deltaEl.textContent = `${latestDelta >= 0 ? "+" : ""}${formatScore(latestDelta)}`;
  deltaEl.className = `metric-value ${deltaTone}`;
  document.getElementById("selected-publication-date").textContent = formatDate(country.latest_publication_date);
  document.getElementById("selected-publication-score").textContent =
    `最近发布日原始均值：${formatScore(country.latest_raw)} · ${scoreDeltaSentence(latestDelta)}`;
  document.getElementById("selected-coverage").textContent =
    `${formatDate(country.start_date)} — ${formatDate(country.latest_date)}`;
  document.getElementById("selected-publication-days").textContent =
    `发布日 ${country.publication_days} 天 · 日历序列 ${country.calendar_days} 天`;
}

function renderDownloadList() {
  const list = document.getElementById("download-list");
  list.innerHTML = "";

  const masterDownloads = [
    {
      title: "全量日度数据（CSV）",
      meta: "包含全部国家的日历日序列、原始发布日均值与 7 日平滑值",
      href: "data/wdsi_all_countries.csv",
    },
    {
      title: "站点摘要（JSON）",
      meta: "国家列表、最新值、覆盖区间与下载入口索引",
      href: summaryPath,
    },
    {
      title: "事件标注（JSON）",
      meta: "供图表添加关键时间点参考的公共事件列表",
      href: eventsPath,
    },
  ];

  const countryDownloads = state.summary.countries.flatMap((country) => [
    {
      title: `${country.label_zh} 数据（CSV）`,
      meta: `${country.label} · 发布日 ${country.publication_days} 天`,
      href: country.file_csv,
    },
    {
      title: `${country.label_zh} 数据（JSON）`,
      meta: "适合网页或脚本直接读取",
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
      <a class="download-link" href="${item.href}" target="_blank" rel="noreferrer">打开文件</a>
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
    chip.textContent = `${event.date} · ${event.title_zh}`;
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
  const rollingValues = records.map((record) => record.rolling7);
  const rawRecords = records.filter((record) => record.publication && record.raw !== null);
  const rawDates = rawRecords.map((record) => record.date);
  const rawValues = rawRecords.map((record) => record.raw);
  const showRolling = state.seriesMode === "rolling";

  const traces = [
    {
      x: rollingDates,
      y: rollingValues,
      type: "scatter",
      mode: "lines",
      name: "7 日平滑指数",
      line: {
        color: country.color,
        width: showRolling ? 2.8 : 1.6,
        dash: showRolling ? "solid" : "dot",
      },
      opacity: showRolling ? 1 : 0.38,
      hovertemplate: "%{x}<br>7 日平滑值：%{y:.3f}<extra></extra>",
    },
    {
      x: rawDates,
      y: rawValues,
      type: "scatter",
      mode: "markers",
      name: "发布日原始均值",
      marker: {
        color: "#b85f35",
        size: showRolling ? 6 : 8,
        opacity: showRolling ? 0.28 : 0.88,
      },
      hovertemplate: "%{x}<br>原始发布日均值：%{y:.3f}<extra></extra>",
    },
  ];

  Plotly.react("wdsi-chart", traces, buildChartLayout(country, countryData), {
    displayModeBar: true,
    responsive: true,
    displaylogo: false,
    modeBarButtonsToRemove: ["select2d", "lasso2d", "autoScale2d"],
  });

  document.getElementById("chart-caption").textContent =
    `${country.label_zh} 的 WDSI 由发布日原始均值和 7 日平滑值共同组成。当前视图重点展示：${
      showRolling ? "更平滑的趋势线" : "发布日上的离散变化"
    }。`;

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
    renderDownloadList();
    bindSeriesToggle();

    if (state.selectedCode) {
      await setSelectedCountry(state.selectedCode);
    }
  } catch (error) {
    document.getElementById("chart-caption").textContent =
      "站点数据加载失败。请确认你正在通过本地服务器或 GitHub Pages 访问，而不是直接双击 HTML 文件。";
    console.error(error);
  }
}

window.addEventListener("DOMContentLoaded", init);
