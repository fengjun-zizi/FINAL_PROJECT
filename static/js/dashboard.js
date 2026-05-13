/* =============================================================
   DVD Rental Dashboard — frontend renderer
   Exposes `window.Dashboard` so the AI chat can call into it.
   ============================================================= */

const Dashboard = (() => {
  let DATA = null;       // full /api/dashboard payload
  let CURRENT_THEME = "dark";
  const VISUAL_STATE_KEY = "dvdDashboard.visualState";
  const AI_OUTPUTS_STATE_KEY = "dvdDashboard.aiOutputs";
  const CHART_TYPE_STATE_KEY = "dvdDashboard.chartTypes";
  let AI_OUTPUTS = [];
  let CHART_TYPES = {};
  const CUSTOM_THEME_VARS = [
    "--bg", "--surface", "--card", "--card2", "--border", "--text", "--muted",
    "--muted2", "--accent", "--accent2", "--accent3", "--teal", "--purple",
    "--red", "--blue", "--grid",
  ];
  const COLOR_ALIASES = {
    green: "#22c55e",
    red: "#ef4444",
    blue: "#3b82f6",
    purple: "#8b5cf6",
    pink: "#ec4899",
    yellow: "#eab308",
    orange: "#f97316",
    teal: "#14b8a6",
    cyan: "#06b6d4",
    gray: "#64748b",
    grey: "#64748b",
    black: "#111827",
    white: "#f8fafc",
    gold: "#d4a017",
    emerald: "#10b981",
    lime: "#84cc16",
    lavender: "#e6e6fa",
    coral: "#ff7f50",
    navy: "#000080",
    maroon: "#800000",
    olive: "#808000",
    beige: "#f5f5dc",
    hijau: "#22c55e",
    merah: "#ef4444",
    biru: "#3b82f6",
    ungu: "#8b5cf6",
    pink: "#ec4899",
    merahmuda: "#ec4899",
    kuning: "#eab308",
    oranye: "#f97316",
    coklat: "#92400e",
    krem: "#f5f5dc",
    hitam: "#111827",
    putih: "#f8fafc",
    "abu-abu": "#64748b",
    abuabu: "#64748b",
    "\u7eff\u8272": "#22c55e",
    "\u7eff": "#22c55e",
    "\u7ea2\u8272": "#ef4444",
    "\u7ea2": "#ef4444",
    "\u84dd\u8272": "#3b82f6",
    "\u84dd": "#3b82f6",
    "\u7d2b\u8272": "#8b5cf6",
    "\u7d2b": "#8b5cf6",
    "\u9ec4\u8272": "#eab308",
    "\u9ec4": "#eab308",
    "\u6a59\u8272": "#f97316",
    "\u6a59": "#f97316",
    "\u7c89\u8272": "#ec4899",
    "\u7c89": "#ec4899",
    "\u9752\u8272": "#06b6d4",
    "\u9752": "#06b6d4",
    "\u68d5\u8272": "#92400e",
    "\u68d5": "#92400e",
    "\u6d45\u7eff\u8272": "#86efac",
    "\u6d45\u7eff": "#86efac",
    "\u6df1\u7eff\u8272": "#166534",
    "\u6df1\u7eff": "#166534",
    "\u6d45\u84dd\u8272": "#93c5fd",
    "\u6d45\u84dd": "#93c5fd",
    "\u6df1\u84dd\u8272": "#1e3a8a",
    "\u6df1\u84dd": "#1e3a8a",
    "\u9ed1\u8272": "#111827",
    "\u9ed1": "#111827",
    "\u767d\u8272": "#f8fafc",
    "\u767d": "#f8fafc",
    "\u7070\u8272": "#64748b",
    "\u7070": "#64748b",
    "\u91d1\u8272": "#d4a017",
  };

  function clamp(n, min, max) {
    return Math.min(max, Math.max(min, n));
  }

  function mixRgb(a, b, amount) {
    return {
      r: Math.round(a.r + (b.r - a.r) * amount),
      g: Math.round(a.g + (b.g - a.g) * amount),
      b: Math.round(a.b + (b.b - a.b) * amount),
    };
  }

  function rgbToHex(rgb) {
    const toHex = value => clamp(value, 0, 255).toString(16).padStart(2, "0");
    return `#${toHex(rgb.r)}${toHex(rgb.g)}${toHex(rgb.b)}`;
  }

  function luminance(rgb) {
    return (0.2126 * rgb.r + 0.7152 * rgb.g + 0.0722 * rgb.b) / 255;
  }

  function clearCustomPalette() {
    CUSTOM_THEME_VARS.forEach(name => document.body.style.removeProperty(name));
  }

  function withUpdatedAt(state) {
    return { ...state, updatedAt: Date.now() };
  }

  function saveVisualState(state) {
    const nextState = withUpdatedAt(state);
    try {
      window.localStorage.setItem(VISUAL_STATE_KEY, JSON.stringify(nextState));
    } catch (e) {
      console.warn("Unable to persist dashboard visual state:", e);
    }

    fetch("/api/visual-state", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(nextState),
    }).catch(e => {
      console.warn("Unable to persist dashboard visual state on server:", e);
    });
  }

  function readVisualState() {
    try {
      const raw = window.localStorage.getItem(VISUAL_STATE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      console.warn("Unable to read dashboard visual state:", e);
      return null;
    }
  }

  async function readServerVisualState() {
    try {
      const res = await fetch("/api/visual-state", { cache: "no-store" });
      if (!res.ok) return null;
      return await res.json();
    } catch (e) {
      console.warn("Unable to read dashboard visual state from server:", e);
      return null;
    }
  }

  function chooseVisualState(localState, serverState) {
    if (!localState) return serverState;
    if (!serverState) return localState;

    const localUpdated = Number(localState.updatedAt || 0);
    const serverUpdated = Number(serverState.updatedAt || 0);
    if (localUpdated || serverUpdated) {
      return localUpdated >= serverUpdated ? localState : serverState;
    }
    return localState;
  }

  function applyVisualState(state) {
    if (state?.mode === "custom-background" && state.color) {
      return setBackgroundColor(state.color, { persist: false });
    }
    if (state?.mode === "theme" && state.theme) {
      return setTheme(state.theme, { persist: false });
    }
    return setTheme("dark", { persist: false });
  }

  async function restoreVisualState() {
    const localState = readVisualState();
    const serverState = await readServerVisualState();
    return applyVisualState(chooseVisualState(localState, serverState));
  }

  function saveAiOutputs() {
    const state = { items: AI_OUTPUTS.slice(-8), updatedAt: Date.now() };
    try {
      window.localStorage.setItem(AI_OUTPUTS_STATE_KEY, JSON.stringify(state));
    } catch (e) {
      console.warn("Unable to persist AI outputs locally:", e);
    }

    fetch("/api/ai-outputs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(state),
    }).catch(e => {
      console.warn("Unable to persist AI outputs on server:", e);
    });
  }

  function readLocalAiOutputs() {
    try {
      const raw = window.localStorage.getItem(AI_OUTPUTS_STATE_KEY);
      return raw ? JSON.parse(raw) : null;
    } catch (e) {
      console.warn("Unable to read local AI outputs:", e);
      return null;
    }
  }

  async function readServerAiOutputs() {
    try {
      const res = await fetch("/api/ai-outputs", { cache: "no-store" });
      if (!res.ok) return null;
      return await res.json();
    } catch (e) {
      console.warn("Unable to read server AI outputs:", e);
      return null;
    }
  }

  function chooseAiOutputs(localState, serverState) {
    if (!localState) return serverState;
    if (!serverState) return localState;
    return Number(localState.updatedAt || 0) >= Number(serverState.updatedAt || 0)
      ? localState
      : serverState;
  }

  async function restoreAiOutputs() {
    const state = chooseAiOutputs(readLocalAiOutputs(), await readServerAiOutputs());
    AI_OUTPUTS = Array.isArray(state?.items) ? state.items.slice(-8) : [];
    renderAiOutputs();
  }

  function normalizeChartId(chart) {
    const key = String(chart || "")
      .trim()
      .toLowerCase()
      .replace(/[\s-]+/g, "_");
    const aliases = {
      revenue: "monthly_revenue_per_store",
      monthly_revenue: "monthly_revenue_per_store",
      monthly_revenue_store: "monthly_revenue_per_store",
      monthly_revenue_per_store: "monthly_revenue_per_store",
      revenue_store: "monthly_revenue_per_store",
      revenue_trend: "monthly_revenue_per_store",
      chart_revenue_store: "monthly_revenue_per_store",
      rental: "monthly_rental_trend",
      monthly_rental: "monthly_rental_trend",
      monthly_rental_trend: "monthly_rental_trend",
      rental_trend: "monthly_rental_trend",
      chart_monthly_rental: "monthly_rental_trend",
    };
    return aliases[key] || key || "monthly_revenue_per_store";
  }

  function normalizeChartType(type) {
    const key = String(type || "")
      .trim()
      .toLowerCase()
      .replace(/[\s-]+/g, "_");
    const aliases = {
      column: "bar",
      columns: "bar",
      batang: "bar",
      kolom: "bar",
      bar_chart: "bar",
      line_chart: "line",
      garis: "line",
      area_chart: "area",
      area: "area",
      scatter_plot: "scatter",
      scatter_chart: "scatter",
      point: "scatter",
      points: "scatter",
      titik: "scatter",
      sebar: "scatter",
      "\u67f1\u72b6\u56fe": "bar",
      "\u67f1\u5f62\u56fe": "bar",
      "\u6761\u5f62\u56fe": "bar",
      "\u6298\u7ebf\u56fe": "line",
      "\u7ebf\u56fe": "line",
      "\u9762\u79ef\u56fe": "area",
      "\u6563\u70b9\u56fe": "scatter",
      pie_chart: "pie",
      donut_chart: "donut",
      doughnut_chart: "donut",
      pie: "pie",
      donut: "donut",
      doughnut: "donut",
      "\u997c\u56fe": "pie",
      "\u73af\u5f62\u56fe": "donut",
    };
    const normalized = aliases[key] || key;
    return ["line", "bar", "area", "scatter", "pie", "donut"].includes(normalized) ? normalized : null;
  }

  function readChartTypes() {
    try {
      const raw = window.localStorage.getItem(CHART_TYPE_STATE_KEY);
      const parsed = raw ? JSON.parse(raw) : {};
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch (e) {
      console.warn("Unable to read chart type state:", e);
      return {};
    }
  }

  function saveChartTypes() {
    try {
      window.localStorage.setItem(CHART_TYPE_STATE_KEY, JSON.stringify(CHART_TYPES));
    } catch (e) {
      console.warn("Unable to persist chart type state:", e);
    }
  }

  function chartTypeFor(chart, fallback = "line") {
    return normalizeChartType(CHART_TYPES[normalizeChartId(chart)]) || fallback;
  }

  function setChartType(chart, type, options = {}) {
    const chartId = normalizeChartId(chart);
    const chartType = normalizeChartType(type);
    if (!chartType) return false;

    CHART_TYPES = { ...CHART_TYPES, [chartId]: chartType };
    if (options.persist !== false) saveChartTypes();
    if (DATA) {
      renderAll();
      renderAiOutputs();
    }

      const elementMap = {
        monthly_revenue: "chart-revenue-highlight",
        monthly_revenue_per_store: "chart-revenue-store",
        monthly_rental_trend: "chart-monthly-rental",
      };
    const el = document.getElementById(elementMap[chartId]);
    if (el && options.scroll !== false) el.scrollIntoView({ behavior: "smooth", block: "center" });
    return true;
  }

  function normalizeColorInput(color) {
    return String(color || "")
      .trim()
      .toLowerCase()
      .replace(/\b(background|bg|page|site|website|theme|color|colour)\b/g, "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function resolveCssColor(color) {
    const normalized = normalizeColorInput(color);
    if (!normalized) return null;

    const candidates = [
      COLOR_ALIASES[normalized],
      COLOR_ALIASES[normalized.replace(/\s+/g, "")],
      normalized,
      normalized.replace(/\s+/g, ""),
    ].filter(Boolean);

    const probe = document.createElement("span");
    let accepted = "";
    for (const candidate of candidates) {
      probe.style.color = "";
      probe.style.color = candidate;
      if (probe.style.color) {
        accepted = candidate;
        break;
      }
    }
    if (!accepted) return null;

    document.body.appendChild(probe);
    const resolved = getComputedStyle(probe).color;
    probe.remove();

    const match = resolved.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/i);
    if (!match) return null;
    return { r: Number(match[1]), g: Number(match[2]), b: Number(match[3]) };
  }

  function buildCustomPalette(base) {
    const isLight = luminance(base) >= 0.58;

    if (isLight) {
      const bg = mixRgb(base, { r: 255, g: 255, b: 255 }, 0.12);
      return {
        "--bg": rgbToHex(bg),
        "--surface": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.7)),
        "--card": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.8)),
        "--card2": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.62)),
        "--border": rgbToHex(mixRgb(bg, { r: 0, g: 0, b: 0 }, 0.18)),
        "--text": "#14202b",
        "--muted": rgbToHex(mixRgb(bg, { r: 0, g: 0, b: 0 }, 0.4)),
        "--muted2": rgbToHex(mixRgb(bg, { r: 0, g: 0, b: 0 }, 0.28)),
        "--accent": rgbToHex(mixRgb(base, { r: 0, g: 0, b: 0 }, 0.24)),
        "--accent2": rgbToHex(mixRgb(base, { r: 0, g: 0, b: 0 }, 0.36)),
        "--accent3": rgbToHex(mixRgb(base, { r: 255, g: 255, b: 255 }, 0.38)),
        "--teal": "#0f766e",
        "--purple": "#7c3aed",
        "--red": "#dc2626",
        "--blue": "#2563eb",
        "--grid": rgbToHex(mixRgb(bg, { r: 0, g: 0, b: 0 }, 0.1)),
      };
    }

    const bg = mixRgb(base, { r: 0, g: 0, b: 0 }, 0.52);
    return {
      "--bg": rgbToHex(bg),
      "--surface": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.05)),
      "--card": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.1)),
      "--card2": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.15)),
      "--border": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.18)),
      "--text": "#eef4ff",
      "--muted": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.38)),
      "--muted2": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.56)),
      "--accent": rgbToHex(mixRgb(base, { r: 255, g: 255, b: 255 }, 0.18)),
      "--accent2": rgbToHex(mixRgb(base, { r: 255, g: 255, b: 255 }, 0.06)),
      "--accent3": rgbToHex(mixRgb(base, { r: 255, g: 255, b: 255 }, 0.4)),
      "--teal": "#2dd4bf",
      "--purple": "#a78bfa",
      "--red": "#f87171",
      "--blue": "#60a5fa",
      "--grid": rgbToHex(mixRgb(bg, { r: 255, g: 255, b: 255 }, 0.08)),
    };
  }

  function setBackgroundColor(color, options = {}) {
    const resolved = resolveCssColor(color);
    if (!resolved) return false;

    CURRENT_THEME = "custom";
    document.body.setAttribute("data-theme", "custom");
    document.querySelectorAll(".theme-btn").forEach(btn => btn.classList.remove("active"));
    clearCustomPalette();
    Object.entries(buildCustomPalette(resolved)).forEach(([name, value]) => {
      document.body.style.setProperty(name, value);
    });
    if (options.persist !== false) {
      saveVisualState({ mode: "custom-background", color: String(color || "").trim() });
    }
    if (DATA) renderAll();
    return true;
  }

  // Plotly base layout pulled from CSS variables for live theme support
  function plotlyBase() {
    const css = getComputedStyle(document.body);
    const text = css.getPropertyValue("--text").trim() || "#eaecf4";
    const accent = css.getPropertyValue("--accent").trim() || "#f5c842";
    const grid = css.getPropertyValue("--grid").trim() || "#1e2235";
    const border = css.getPropertyValue("--border").trim() || "#242840";
    return {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { family: "DM Sans", color: text, size: 12 },
      title: { font: { family: "Playfair Display", color: accent, size: 15 } },
      legend: {
        bgcolor: "rgba(22,27,42,0.7)", bordercolor: border, borderwidth: 1,
        font: { size: 11, color: text },
      },
      margin: { l: 50, r: 20, t: 50, b: 50 },
      colorway: ["#f5c842", "#2dd4bf", "#a78bfa", "#f87171", "#fb923c", "#34d399", "#60a5fa"],
      xaxis: { gridcolor: grid, zerolinecolor: grid, linecolor: border, tickfont: { color: text } },
      yaxis: { gridcolor: grid, zerolinecolor: grid, linecolor: border, tickfont: { color: text } },
    };
  }

  function plot(id, data, layoutOverride = {}) {
    const el = document.getElementById(id);
    if (!el) return;
    if (!window.Plotly) {
      el.innerHTML = `<div style="padding:1rem;color:var(--red)">Plotly failed to load. Check the CDN script in templates/index.html.</div>`;
      return;
    }
    const base = plotlyBase();
    // Remove any hardcoded height from override — let CSS control height
    const { height: _h, ...safeOverride } = layoutOverride;
    const layout = {
      ...base,
      ...safeOverride,
      autosize: true,
      xaxis: { ...base.xaxis, ...(safeOverride.xaxis || {}) },
      yaxis: { ...base.yaxis, ...(safeOverride.yaxis || {}) },
    };
    try {
      Plotly.react(el, data, layout, {
        responsive: true,
        displaylogo: false,
        useResizeHandler: true,
      });
    } catch (e) {
      console.error(`Failed to render chart ${id}:`, e);
      el.innerHTML = `<div style="padding:1rem;color:var(--red)">Chart render failed: ${e.message}</div>`;
    }
  }

  // ----- KPI builder -----
  function kpiCard(label, value, note, color = "gold", icon = "📊") {
    return `
      <div class="kpi ${color}" data-kpi="${label.toLowerCase().replace(/\s+/g, "_")}">
        <div class="kpi-icon">${icon}</div>
        <div class="kpi-lbl">${label}</div>
        <div class="kpi-val">${value}</div>
        <div class="kpi-note">${note}</div>
      </div>`;
  }

  function renderKPIs() {
    const o = DATA.kpi.overview;
    document.getElementById("kpi-overview").innerHTML = [
      kpiCard("Total Films", o.total_film.toLocaleString(), "Entire catalog", "gold", "🎥"),
      kpiCard("Total Genres", o.total_genre, "Unique categories", "teal", "🎭"),
      kpiCard("Avg Duration", `${o.avg_duration} <small>min</small>`, "Per film", "purple", "⏱️"),
      kpiCard("Avg Rental Rate", `$${o.avg_rate}`, "Average price", "red", "💵"),
    ].join("");

    const p = DATA.kpi.popularity;
    document.getElementById("kpi-popularity").innerHTML = [
      kpiCard("Total Rentals", p.total_rental.toLocaleString(), "📼 All transactions", "gold", "📼"),
      kpiCard("Active Customers", p.active_customers.toLocaleString(), "👤 Unique renters", "teal", "👤"),
      kpiCard("Avg Rental Days", `${p.avg_days} <small>days</small>`, "⏳ Per transaction", "purple", "⏳"),
      kpiCard("Not Returned", p.not_returned.toLocaleString(), "⚠️ Still with customers", "red", "⚠️"),
      kpiCard("Active Days", p.active_days, "📅 Days with transactions", "blue", "📅"),
    ].join("");

    const a = DATA.kpi.actor;
    document.getElementById("kpi-actor").innerHTML = [
      kpiCard("Total Actors", a.total_actors, "In the database", "gold", "🌟"),
      kpiCard("Total Films", a.total_films.toLocaleString(), "In the catalog", "teal", "🎥"),
      kpiCard("Total Genres", a.total_genres, "Unique categories", "purple", "🎭"),
    ].join("");

    const r = DATA.kpi.revenue;
    document.getElementById("kpi-revenue").innerHTML = [
      kpiCard("Total Revenue", `$${r.total_rev.toLocaleString()}`, "All payments", "gold", "💰"),
      kpiCard("Total Transactions", r.total_payment.toLocaleString(), "Payment records", "teal", "🧾"),
      kpiCard("Avg Transaction", `$${r.avg_payment}`, "Per payment", "purple", "📊"),
      kpiCard("Paying Customers", r.total_cust.toLocaleString(), "Unique customers", "red", "👥"),
    ].join("");
  }

  // ----- chart renderers (each takes data array, draws into an element id) -----

  function chartGenreBar(rows, target = "chart-genre-bar") {
    plot(target, [{
      type: "bar",
      x: rows.map(r => r.genre),
      y: rows.map(r => r.film_count),
      text: rows.map(r => r.film_count),
      textposition: "outside",
      marker: {
        color: rows.map(r => r.film_count),
        colorscale: [[0, "#1c2138"], [1, "#f5c842"]],
      },
    }], { title: "Films per Genre", xaxis: { tickangle: -35 } });
  }

  function chartGenrePie(rows, target = "chart-genre-pie") {
    plot(target, [{
      type: "pie",
      labels: rows.map(r => r.genre),
      values: rows.map(r => r.film_count),
      hole: 0.5,
      textinfo: "percent+label",
      textfont: { size: 10 },
    }], { title: "Genre Share", showlegend: false });
  }

  function chartRatingPie(rows, target = "chart-rating-pie") {
    plot(target, [{
      type: "pie",
      labels: rows.map(r => r.rating),
      values: rows.map(r => r.count),
      hole: 0.55,
      textinfo: "percent+label",
      marker: { colors: ["#f5c842", "#2dd4bf", "#a78bfa", "#f87171", "#fb923c"] },
    }], { title: "Rating Distribution", showlegend: false });
  }

  function chartDurationGenre(rows, target = "chart-duration-genre") {
    const sorted = [...rows].sort((a, b) => a.avg_duration - b.avg_duration);
    plot(target, [{
      type: "bar",
      orientation: "h",
      x: sorted.map(r => r.avg_duration),
      y: sorted.map(r => r.genre),
      text: sorted.map(r => r.avg_duration),
      textposition: "outside",
      marker: {
        color: sorted.map(r => r.avg_duration),
        colorscale: [[0, "#1c2138"], [1, "#2dd4bf"]],
      },
    }], { title: "Avg Duration per Genre (min)" });
  }

  function chartRentalPeriod(rows, target = "chart-rental-period") {
    plot(target, [{
      type: "bar",
      x: rows.map(r => r.rental_period),
      y: rows.map(r => r.film_count),
      text: rows.map(r => r.film_count),
      textposition: "outside",
      marker: {
        color: rows.map(r => r.film_count),
        colorscale: [[0, "#1c2138"], [1, "#a78bfa"]],
      },
    }], { title: "Films by Rental Period" });
  }

  function chartGenreRating(rows, target = "chart-genre-rating") {
    const genres = [...new Set(rows.map(r => r.genre))].sort();
    const ratings = [...new Set(rows.map(r => r.rating))].sort();
    const z = genres.map(g => ratings.map(r => {
      const m = rows.find(x => x.genre === g && x.rating === r);
      return m ? m.count : 0;
    }));
    plot(target, [{
      type: "heatmap",
      x: ratings, y: genres, z,
      text: z, texttemplate: "%{text}",
      colorscale: [[0, "#0b0d12"], [1, "#f5c842"]],
      showscale: false,
    }], { title: "Film Count: Genre × Rating" });
  }

  function chartGenreExtremes(rows, target = "chart-genre-extremes") {
    const longest = rows.filter(r => r.type === "Longest");
    const shortest = rows.filter(r => r.type === "Shortest");
    const order = [...new Set(longest.map(r => r.genre))].sort();
    plot(target, [
      {
        type: "bar", orientation: "h",
        name: "Longest",
        x: order.map(g => (longest.find(r => r.genre === g) || {}).duration || 0),
        y: order, text: order.map(g => (longest.find(r => r.genre === g) || {}).title || ""),
        textposition: "outside", textfont: { size: 9 },
        marker: { color: "#f5c842" },
      },
      {
        type: "bar", orientation: "h",
        name: "Shortest",
        x: order.map(g => (shortest.find(r => r.genre === g) || {}).duration || 0),
        y: order, text: order.map(g => (shortest.find(r => r.genre === g) || {}).title || ""),
        textposition: "outside", textfont: { size: 9 },
        marker: { color: "#2dd4bf" },
      },
    ], { title: "Longest vs Shortest Film per Genre", barmode: "group", height: 600 });
  }

  function formatNumber(value) {
    return Number(value || 0).toLocaleString();
  }

  function formatMetricValue(value, metricLabel = "Value") {
    const numeric = Number(value || 0);
    return /revenue|\(\$\)|\$/i.test(metricLabel)
      ? `$${numeric.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
      : formatNumber(Math.round(numeric * 100) / 100);
  }

  function monthLabel(value) {
    const [year, month] = String(value || "").split("-");
    if (!year || !month) return String(value || "");
    return new Date(Number(year), Number(month) - 1, 1).toLocaleString("en-US", {
      month: "short",
      year: "numeric",
    });
  }

  function chartTopRented(rows, target = "chart-top-rented", titleOverride = "") {
      const sorted = [...rows].sort((a, b) => a.total_rentals - b.total_rentals);
      const title = titleOverride || `Top ${sorted.length} Rented Films - ranked by total rentals`;
      plot(target, [{
        type: "bar",
        orientation: "h",
      x: sorted.map(r => r.total_rentals),
      y: sorted.map(r => r.title),
      text: sorted.map(r => formatNumber(r.total_rentals)),
      textposition: "outside",
      customdata: sorted.map(r => [r.genre, r.rating, r.rental_rate]),
      hovertemplate:
        "<b>%{y}</b><br>Rentals: %{x:,}<br>Genre: %{customdata[0]}<br>Rating: %{customdata[1]}<br>Rental rate: $%{customdata[2]}<extra></extra>",
      marker: {
        color: sorted.map(r => r.total_rentals),
        colorscale: [[0, "#2dd4bf"], [0.55, "#f5c842"], [1, "#fb923c"]],
        line: { color: "rgba(255,255,255,0.22)", width: 1 },
      },
      }], {
        title,
        showlegend: false,
        margin: { l: 170, r: 55, t: 55, b: 55 },
        xaxis: { title: "Total rentals" },
        yaxis: { title: "", automargin: true },
      });
  }

  function chartMlPopularity(payload, target = "chart-ml-popularity") {
    const scoreOf = row => Number(row.opportunity_score ?? row.predicted_rentals ?? 0);
    const demandOf = row => Number(row.predicted_demand ?? row.predicted_rentals ?? 0);
    const rows = [...(payload?.predictions || [])].sort((a, b) => scoreOf(a) - scoreOf(b));
    const colors = rows.map(r => {
      if (r.risk === "Stock risk") return "#fb923c";
      if (r.risk === "Demand drop risk") return "#f87171";
      if (r.recommendation === "Promote next month") return "#2dd4bf";
      return "#f5c842";
    });

    plot(target, [{
      type: "bar",
      orientation: "h",
      x: rows.map(scoreOf),
      y: rows.map(r => r.title),
      text: rows.map(r => `${scoreOf(r)}/100`),
      textposition: "outside",
      customdata: rows.map(r => [r.genre, r.rating, demandOf(r), r.recommendation, r.risk, r.confidence]),
      hovertemplate:
        "<b>%{y}</b><br>Opportunity score: %{x}/100<br>Predicted demand: %{customdata[2]} rentals<br>Recommendation: %{customdata[3]}<br>Risk: %{customdata[4]}<br>Genre: %{customdata[0]}<br>Rating: %{customdata[1]}<br>Confidence: %{customdata[5]}%<extra></extra>",
      marker: { color: colors },
    }], {
      title: `Demand Opportunity Forecast for ${payload?.next_month || "Next Month"}`,
      margin: { l: 150, r: 35, t: 55, b: 45 },
      xaxis: { title: "Opportunity score (0-100)", range: [0, 105] },
    });
  }

  function renderMlPredictionSummary(payload) {
    const el = document.getElementById("ml-prediction-summary");
    if (!el) return;
    const rows = payload?.predictions || [];
    if (!rows.length) {
      el.innerHTML = `<div class="prediction-meta">No prediction data is available.</div>`;
      return;
    }

    el.innerHTML = `
      <div class="prediction-summary">
        <div class="prediction-meta">
          <b>Model:</b> ${escapeHtml(payload.model || "demand_opportunity_forecast")}<br>
          <b>Target month:</b> ${escapeHtml(payload.next_month || "next month")}<br>
          ${escapeHtml(payload.explanation || "Scores demand opportunity and recommended business action.")}
        </div>
        ${rows.slice(0, 5).map(row => `
          <div class="prediction-row">
            <div class="prediction-rank">${row.rank}</div>
            <div>
              <div class="prediction-title">${escapeHtml(row.title)}</div>
              <div class="prediction-sub prediction-decision">${escapeHtml(row.genre)} · ${escapeHtml(row.rating)} · ${escapeHtml(row.recommendation)} · ${escapeHtml(row.risk)}</div>
              <div class="prediction-sub">${escapeHtml(row.genre)} · ${escapeHtml(row.rating)} · ${escapeHtml(row.trend)} trend · ${row.confidence}% confidence</div>
            </div>
            <div class="prediction-score">${row.opportunity_score ?? row.predicted_rentals}<span>score</span></div>
          </div>
        `).join("")}
      </div>`;
  }

  function renderPopularityAdvisorResult(payload) {
    const el = document.getElementById("popularity-advisor-output");
    if (!el) return;
    const rows = payload?.recommendations || [];
    const unmatched = payload?.unmatched || [];

    if (!rows.length) {
      el.innerHTML = `
        <div class="prediction-meta">
          No matching films or genres were found. Try exact film titles like <b>ACADEMY DINOSAUR</b>, genres like <b>Action</b>, or ratings like <b>PG-13</b>.
          ${unmatched.length ? `<br><b>Unmatched:</b> ${unmatched.map(escapeHtml).join(", ")}` : ""}
        </div>`;
      return;
    }

    el.innerHTML = `
      <div class="prediction-meta">
        <b>Model:</b> ${escapeHtml(payload.model || "candidate_demand_opportunity_score")}<br>
        ${escapeHtml(payload.message || "Scores combine demand opportunity, recent rentals, historical demand, stock, and price.")}
        ${unmatched.length ? `<br><b>Unmatched:</b> ${unmatched.map(escapeHtml).join(", ")}` : ""}
      </div>
      <div class="advisor-table-wrap">
        <table class="advisor-table">
          <thead>
            <tr>
              <th>Rank</th>
              <th>Candidate</th>
              <th>Decision Score</th>
              <th>Opportunity</th>
              <th>Risk</th>
              <th>Recommendation</th>
              <th>Reason</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map(row => `
              <tr>
                <td>${row.rank}</td>
                <td>
                  <b>${escapeHtml(row.title)}</b><br>
                  <span class="prediction-sub">${escapeHtml(row.genre)} · ${escapeHtml(row.rating)} · matched: ${escapeHtml(row.matched_input)}</span>
                </td>
                <td><span class="advisor-badge">${row.score}</span></td>
                <td>${row.opportunity_score || 0}/100</td>
                <td>${escapeHtml(row.risk || "Unknown risk")}</td>
                <td>${escapeHtml(row.recommendation)}</td>
                <td>${escapeHtml(row.reason)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>`;
  }

  function renderLocalTransformerStatus(status) {
    const el = document.getElementById("ml-transformer-status");
    if (!el) return;
    if (!status) {
      el.innerHTML = `<div class="ml-status-card">No local Transformer status is available yet.</div>`;
      return;
    }

    const ready = Boolean(status.ready);
    const stateLabel = ready ? "Ready" : status.enabled ? "Configured but unavailable" : "Disabled";
    const reason = status.reason ? `<div class="ml-status-card">${escapeHtml(status.reason)}</div>` : "";
    const labels = Array.isArray(status.default_labels) && status.default_labels.length
      ? `<div class="ml-status-card"><b>Default labels:</b> ${status.default_labels.map(escapeHtml).join(", ")}</div>`
      : "";

    el.innerHTML = `
      <div class="ml-state-pill ${ready ? "ready" : "offline"}">${stateLabel}</div>
      <div class="ml-status-card">
        <b>Task:</b> ${escapeHtml(status.task || "unknown")}<br>
        <b>Model source:</b> ${escapeHtml(status.model_source || "not configured")}<br>
        <b>Loaded:</b> ${status.loaded ? "yes" : "no"}
      </div>
      ${reason}
      ${labels}
    `;
  }

  function renderLocalTransformerResult(payload) {
    const el = document.getElementById("ml-transformer-result");
    if (!el) return;
    if (!payload) {
      el.innerHTML = "";
      return;
    }

    if (payload.features) {
      el.innerHTML = `
        <div class="ml-result-card">
          <b>Feature Extraction Result</b><br>
          Token count: ${formatNumber(payload.features.token_count)}<br>
          Embedding dim: ${formatNumber(payload.features.embedding_dim)}<br>
          Mean pool preview: ${escapeHtml((payload.features.mean_pool_preview || []).join(", "))}
        </div>
      `;
      return;
    }

    const predictions = Array.isArray(payload.predictions) ? payload.predictions : [];
    if (!predictions.length) {
      el.innerHTML = `<div class="ml-result-card">No predictions were returned by the local Transformer model.</div>`;
      return;
    }

    el.innerHTML = `
      <div class="ml-result-card">
        <b>Top label:</b> ${escapeHtml(payload.top_label || "-")}<br>
        <b>Top score:</b> ${Number(payload.top_score || 0).toFixed(4)}
      </div>
      <div class="ml-result-card">
        <table class="ml-result-table">
          <thead>
            <tr><th>Label</th><th>Score</th></tr>
          </thead>
          <tbody>
            ${predictions.map(item => `
              <tr>
                <td>${escapeHtml(item.label)}</td>
                <td>${Number(item.score || 0).toFixed(4)}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    `;
  }

  async function runLocalTransformerInference() {
    const textEl = document.getElementById("ml-transformer-text");
    const labelsEl = document.getElementById("ml-transformer-labels");
    const topkEl = document.getElementById("ml-transformer-topk");
    const button = document.getElementById("ml-transformer-run");
    const resultEl = document.getElementById("ml-transformer-result");
    if (!textEl || !labelsEl || !topkEl || !button || !resultEl) return;

    const text = String(textEl.value || "").trim();
    if (!text) {
      resultEl.innerHTML = `<div class="ml-result-card">Please enter some text first.</div>`;
      return;
    }

    const labels = String(labelsEl.value || "")
      .split(",")
      .map(item => item.trim())
      .filter(Boolean);
    const topK = Number(topkEl.value || 3);
    button.disabled = true;
    resultEl.innerHTML = `<div class="ml-result-card">Running local Transformer inference...</div>`;

    try {
      const res = await fetch("/api/ml/local-transformer/predict", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text, labels, top_k: topK }),
      });
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.detail || "Local Transformer inference failed.");
      renderLocalTransformerResult(payload);
    } catch (e) {
      resultEl.innerHTML = `<div class="ml-result-card" style="color:var(--red)">Transformer inference failed: ${escapeHtml(e.message)}</div>`;
    } finally {
      button.disabled = false;
    }
  }

  function renderForecastSummary(payload) {
    const el = document.getElementById("ml-forecast-summary");
    if (!el) return;
    const summary = payload?.summary;
    if (!summary) {
      el.innerHTML = "";
      return;
    }

    const metricLabel = payload.metric_label || "Value";
    el.innerHTML = `
      <div><b>Latest month</b><span>${monthLabel(summary.latest_month)} - ${formatMetricValue(summary.latest_value, metricLabel)}</span></div>
      <div><b>Historical average</b><span>${formatMetricValue(summary.average_value, metricLabel)}</span></div>
      <div><b>Detected trend</b><span>${escapeHtml(summary.trend || "stable")} (slope ${Number(summary.slope || 0).toFixed(4)})</span></div>
      <div><b>Forecast horizon</b><span>${formatNumber(summary.forecast_horizon || 0)} month(s)</span></div>
    `;
  }

  function chartMlForecast(payload, target = "chart-ml-forecast") {
    const history = Array.isArray(payload?.history) ? payload.history : [];
    const forecast = Array.isArray(payload?.forecast) ? payload.forecast : [];
    if (!history.length) {
      const el = document.getElementById(target);
      if (el) el.innerHTML = `<div style="padding:1rem;color:var(--muted2)">No time-series data is available.</div>`;
      return;
    }

    const metricLabel = payload.metric_label || "Value";
    const allMonths = [...history.map(row => row.month), ...forecast.map(row => row.month)];
    const title = payload.title || "Time Series Forecast";

    plot(target, [
      {
        type: "scatter",
        mode: "lines+markers+text",
        name: "History",
        x: history.map(row => row.month),
        y: history.map(row => Number(row.value || 0)),
        text: history.map(row => formatMetricValue(row.value, metricLabel)),
        textposition: "top center",
        line: { width: 3, color: "#2dd4bf" },
        marker: { size: 7, color: "#2dd4bf" },
        hovertemplate: "<b>%{x}</b><br>History: %{y:,.2f}<extra></extra>",
      },
      {
        type: "scatter",
        mode: "lines+markers+text",
        name: "Forecast",
        x: forecast.map(row => row.month),
        y: forecast.map(row => Number(row.value || 0)),
        text: forecast.map(row => formatMetricValue(row.value, metricLabel)),
        textposition: "top center",
        line: { width: 3, color: "#f5c842", dash: "dot" },
        marker: { size: 7, color: "#f5c842" },
        hovertemplate: "<b>%{x}</b><br>Forecast: %{y:,.2f}<extra></extra>",
      },
    ], {
      title: `${title} - historical vs forecast`,
      margin: { l: 70, r: 35, t: 60, b: 80 },
      legend: { orientation: "h", x: 0, y: 1.12 },
      xaxis: {
        title: "Month",
        type: "category",
        categoryorder: "array",
        categoryarray: allMonths,
        tickvals: allMonths,
        ticktext: allMonths.map(monthLabel),
        tickangle: allMonths.length > 8 ? -20 : 0,
        automargin: true,
      },
      yaxis: { title: metricLabel, rangemode: "tozero", automargin: true },
    });
  }

  async function runTimeSeriesForecast() {
    const datasetEl = document.getElementById("ml-forecast-dataset");
    const horizonEl = document.getElementById("ml-forecast-horizon");
    const button = document.getElementById("ml-forecast-run");
    const summaryEl = document.getElementById("ml-forecast-summary");
    if (!datasetEl || !horizonEl || !button || !summaryEl) return;

    const dataset = String(datasetEl.value || "monthly_revenue_total");
    const horizon = Number(horizonEl.value || 4);
    button.disabled = true;
    summaryEl.innerHTML = `<div><b>Running forecast</b><span>Preparing ${escapeHtml(dataset)} for the next ${formatNumber(horizon)} month(s).</span></div>`;

    try {
      const res = await fetch("/api/ml/time-series/forecast", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataset, horizon }),
      });
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.detail || "Time-series forecast failed.");
      renderForecastSummary(payload);
      chartMlForecast(payload);
    } catch (e) {
      summaryEl.innerHTML = `<div><b>Forecast failed</b><span style="color:var(--red)">${escapeHtml(e.message)}</span></div>`;
      const el = document.getElementById("chart-ml-forecast");
      if (el) el.innerHTML = "";
    } finally {
      button.disabled = false;
    }
  }

  async function runPopularityAdvisor() {
    const input = document.getElementById("popularity-advisor-input");
    const button = document.getElementById("popularity-advisor-run");
    const output = document.getElementById("popularity-advisor-output");
    if (!input || !button || !output) return;

    const items = input.value
      .split(/\r?\n|,/)
      .map(item => item.trim())
      .filter(Boolean);

    if (!items.length) {
      output.innerHTML = `<div class="prediction-meta">Please enter at least one film title, genre, or rating.</div>`;
      return;
    }

    button.disabled = true;
    output.innerHTML = `<div class="prediction-meta">Running ML analysis for ${items.length} candidate item(s)...</div>`;

    try {
      const res = await fetch("/api/ml/popularity-advisor", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ items, limit: 10 }),
      });
      if (!res.ok) {
        throw new Error(await res.text());
      }
      renderPopularityAdvisorResult(await res.json());
    } catch (e) {
      output.innerHTML = `<div class="prediction-meta" style="color:var(--red)">Advisor failed: ${escapeHtml(e.message)}</div>`;
    } finally {
      button.disabled = false;
    }
  }

  function prepareMonthlyRentalTimeline(rows) {
    const months = [...new Set(rows.map(r => r.month))].sort();
    const stores = [...new Set(rows.map(r => r.store))].sort();
    const monthTotals = months.map(month => {
      const storeValues = Object.fromEntries(stores.map(store => {
        const found = rows.find(r => r.month === month && r.store === store);
        return [store, Number(found?.total || 0)];
      }));
      return {
        month,
        total: Object.values(storeValues).reduce((sum, value) => sum + value, 0),
        storeValues,
      };
    });
    const activeMonths = monthTotals.filter(item => item.total > 0);
    const zeroMonthCount = monthTotals.length - activeMonths.length;
    const shouldCondense = monthTotals.length > 18 && zeroMonthCount >= Math.max(6, Math.floor(monthTotals.length * 0.35));
    const visibleMonthTotals = shouldCondense && activeMonths.length ? activeMonths : monthTotals;

    return {
      stores,
      monthTotals,
      activeMonths,
      zeroMonthCount,
      shouldCondense,
      visibleMonthTotals,
      visibleMonths: visibleMonthTotals.map(item => item.month),
    };
  }

  function renderMonthlyRentalInsight(rows) {
    const el = document.getElementById("monthly-rental-insight");
    if (!el || !rows?.length) return;

    const timeline = prepareMonthlyRentalTimeline(rows);
    const { stores, monthTotals, activeMonths, zeroMonthCount, shouldCondense, visibleMonthTotals } = timeline;
    const baseSeries = activeMonths.length ? activeMonths : monthTotals;
    const peak = baseSeries.reduce((best, item) => item.total > best.total ? item : best, baseSeries[0]);
    const latest = baseSeries[baseSeries.length - 1];
    const latestIndex = monthTotals.findIndex(item => item.month === latest.month);
    const previous = monthTotals[latestIndex - 1] || latest;
    const change = latest.total - previous.total;
    const changeText = `${change >= 0 ? "+" : ""}${formatNumber(change)} vs previous month`;
    const leadingStore = stores
      .map(store => ({ store, total: latest.storeValues[store] || 0 }))
      .sort((a, b) => b.total - a.total)[0];

    el.innerHTML = `
      <div><b>Peak month</b><span>${monthLabel(peak.month)} - ${formatNumber(peak.total)} rentals</span></div>
      <div><b>Latest active month</b><span>${monthLabel(latest.month)} - ${formatNumber(latest.total)} rentals (${changeText})</span></div>
      <div><b>Leading store</b><span>${leadingStore.store} with ${formatNumber(leadingStore.total)} rentals in the latest month</span></div>
      ${zeroMonthCount ? `<div><b>${shouldCondense ? "Hidden empty months" : "Zero-rental months"}</b><span>${zeroMonthCount} month(s) ${shouldCondense ? "are hidden so the chart stays readable" : "are shown as 0 to make the gap visible"}</span></div>` : ""}
    `;
  }

  function chartMonthlyRental(rows, target = "chart-monthly-rental") {
    const timeline = prepareMonthlyRentalTimeline(rows);
    const { stores, visibleMonthTotals, visibleMonths, shouldCondense } = timeline;
    const months = visibleMonths;
    const palette = { "Store 1": "#f5c842", "Store 2": "#2dd4bf" };
    const chartType = chartTypeFor("monthly_rental_trend", "bar");
    const totals = visibleMonthTotals.map(item => item.total);

    renderMonthlyRentalInsight(rows);

    const storeTraces = stores.map(store => {
      const values = visibleMonthTotals.map(item => Number(item.storeValues[store] || 0));
      const isBar = chartType === "bar";
      const isScatter = chartType === "scatter";
      const trace = {
        type: isBar ? "bar" : "scatter",
        name: store,
        x: months,
        y: values,
        customdata: months.map(monthLabel),
        hovertemplate: `<b>${store}</b><br>Month: %{customdata}<br>Rentals: %{y:,}<extra></extra>`,
        marker: {
          color: palette[store] || "#a78bfa",
          line: { color: "rgba(255,255,255,0.2)", width: 1 },
        },
      };
      if (isBar) {
        trace.text = values.map(formatNumber);
        trace.textposition = "outside";
        trace.cliponaxis = false;
      } else {
        trace.mode = isScatter ? "markers+text" : "lines+markers+text";
        trace.text = values.map(value => value > 0 ? formatNumber(value) : "");
        trace.textposition = "top center";
        trace.line = { width: 2.8, color: palette[store] || "#a78bfa" };
        trace.marker = { size: isScatter ? 10 : 7, color: palette[store] || "#a78bfa" };
        if (chartType === "area") trace.fill = "tozeroy";
      }
      return trace;
    });

    const peakIndex = totals.indexOf(Math.max(...totals));
    const totalTrace = {
      type: "scatter",
      mode: chartType === "scatter" ? "markers+text" : "lines+markers+text",
      name: "Total",
      x: months,
      y: totals,
      text: totals.map(value => value > 0 ? formatNumber(value) : ""),
      textposition: "top center",
      customdata: months.map(monthLabel),
      hovertemplate: "<b>Total rentals</b><br>Month: %{customdata}<br>Total: %{y:,}<extra></extra>",
      line: { color: "#f87171", width: 3, dash: "dot" },
      marker: { size: 8, color: "#f87171", line: { color: "#fff", width: 1 } },
    };
    const chartLabel = { line: "line chart", bar: "bar chart", area: "area chart", scatter: "scatter chart" }[chartType];

    plot(target, [...storeTraces, totalTrace], {
      title: `Monthly Rentals by Store - ${chartLabel}${shouldCondense ? "; only active months are shown" : "; total shown in red"}`,
      barmode: chartType === "bar" ? "group" : undefined,
      bargap: chartType === "bar" ? 0.34 : 0.18,
      bargroupgap: chartType === "bar" ? 0.08 : 0,
      margin: { l: 70, r: 35, t: 76, b: 90 },
      legend: { orientation: "h", x: 0, y: 1.14 },
      xaxis: {
        title: "Rental month",
        type: "category",
        categoryorder: "array",
        categoryarray: months,
        tickvals: months,
        ticktext: months.map(monthLabel),
        tickangle: months.length > 10 ? -25 : 0,
        automargin: true,
      },
      yaxis: { title: "Number of rentals", rangemode: "tozero", automargin: true },
      annotations: [{
        x: months[peakIndex],
        y: totals[peakIndex],
        text: `Peak: ${formatNumber(totals[peakIndex])}`,
        showarrow: true,
        arrowhead: 2,
        ax: 0,
        ay: -42,
        font: { size: 12 },
      }],
    });
  }

  function chartRentalDow(rows, target = "chart-rental-dow") {
    const sorted = [...rows].sort((a, b) => Number(a.sort_order || 0) - Number(b.sort_order || 0));
    const maxValue = Math.max(...sorted.map(r => Number(r.total || 0)), 1);
    plot(target, [{
      type: "bar",
      x: sorted.map(r => String(r.day || "").trim()),
      y: sorted.map(r => r.total),
      text: sorted.map(r => formatNumber(r.total)),
      textposition: "outside",
      hovertemplate: "<b>%{x}</b><br>Rentals: %{y:,}<extra></extra>",
      marker: {
        color: sorted.map(r => r.total),
        colorscale: [[0, "#60a5fa"], [0.55, "#a78bfa"], [1, "#f5c842"]],
        line: { color: "rgba(255,255,255,0.2)", width: 1 },
      },
    }], {
      title: "Rental Pattern by Day - identify the strongest rental days",
      showlegend: false,
      margin: { l: 65, r: 35, t: 55, b: 70 },
      xaxis: { title: "Day of week" },
      yaxis: { title: "Total rentals", range: [0, maxValue * 1.18] },
    });
  }

  function chartStoreTop(rows, target = "chart-store-top") {
    const stores = [...new Set(rows.map(r => r.store))];
    const traces = stores.map((s, idx) => {
      const sub = [...rows.filter(r => r.store === s)].sort((a, b) => a.total_rentals - b.total_rentals);
      return {
        type: "bar", orientation: "h",
        name: s,
        x: sub.map(r => r.total_rentals),
        y: sub.map(r => `${r.title} (${s})`),
        text: sub.map(r => formatNumber(r.total_rentals)),
        textposition: "outside",
        customdata: sub.map(r => [r.genre]),
        hovertemplate: `<b>${s}</b><br>%{y}<br>Rentals: %{x:,}<br>Genre: %{customdata[0]}<extra></extra>`,
        marker: { color: idx === 0 ? "#f5c842" : "#2dd4bf" },
      };
    });
    plot(target, traces, {
      title: "Top 5 Films per Store - compare store-specific winners",
      barmode: "group",
      margin: { l: 230, r: 45, t: 55, b: 55 },
      xaxis: { title: "Total rentals" },
      yaxis: { title: "", automargin: true },
    });
  }

  function chartLeastRented(rows, target = "chart-least-rented") {
    const sorted = [...rows].sort((a, b) => a.total_rentals - b.total_rentals);
    plot(target, [{
      type: "bar", orientation: "h",
      x: sorted.map(r => r.total_rentals),
      y: sorted.map(r => r.title),
      text: sorted.map(r => `${formatNumber(r.total_rentals)} rentals`),
      textposition: "outside",
      customdata: sorted.map(r => [r.genre, r.rating, r.stock_units]),
      hovertemplate:
        "<b>%{y}</b><br>Rentals: %{x:,}<br>Genre: %{customdata[0]}<br>Rating: %{customdata[1]}<br>Stock units: %{customdata[2]}<extra></extra>",
      marker: {
        color: sorted.map(r => r.total_rentals),
        colorscale: [[0, "#64748b"], [0.55, "#fb923c"], [1, "#f87171"]],
        line: { color: "rgba(255,255,255,0.18)", width: 1 },
      },
    }], {
      title: "Low-Rental Films - inventory review list",
      showlegend: false,
      margin: { l: 170, r: 70, t: 55, b: 55 },
      xaxis: { title: "Total rentals" },
      yaxis: { title: "", automargin: true },
    });
  }

  function chartActorFilms(rows, target = "chart-actor-films", titleOverride = "") {
      const top = [...rows].sort((a, b) => b.film_count - a.film_count).slice(0, 20).reverse();
      const title = titleOverride || `Top ${top.length} Actors by Film Count`;
      plot(target, [{
        type: "bar", orientation: "h",
        x: top.map(r => r.film_count),
      y: top.map(r => r.actor),
      text: top.map(r => r.film_count),
      textposition: "outside",
      marker: {
        color: top.map(r => r.film_count),
        colorscale: [[0, "#1c2138"], [1, "#a78bfa"]],
      },
      }], { title, height: 560 });
    }
  
  function chartActorRentals(rows, target = "chart-actor-rentals", titleOverride = "") {
      const top = [...rows].sort((a, b) => b.rental_count - a.rental_count).slice(0, 20).reverse();
      const title = titleOverride || `Top ${top.length} Actors by Rental Count`;
      plot(target, [{
        type: "bar", orientation: "h",
        x: top.map(r => r.rental_count),
      y: top.map(r => r.actor),
      text: top.map(r => r.rental_count),
      textposition: "outside",
      marker: {
        color: top.map(r => r.rental_count),
        colorscale: [[0, "#1c2138"], [1, "#f5c842"]],
      },
      }], { title, height: 560 });
    }

  function chartActorGenre(rows, target = "chart-actor-genre") {
    const actors = [...new Set(rows.map(r => r.actor))];
    const genres = [...new Set(rows.map(r => r.genre))];
    const traces = genres.map(g => ({
      type: "bar",
      name: g,
      x: actors,
      y: actors.map(a => {
        const m = rows.find(r => r.actor === a && r.genre === g);
        return m ? m.count : 0;
      }),
      text: actors.map(a => {
        const m = rows.find(r => r.actor === a && r.genre === g);
        return m ? m.count : "";
      }),
      textposition: "outside",
    }));
    plot(target, traces, { title: "Genre Mix — Top 5 Actors", barmode: "group", height: 500 });
  }

  function chartRevenueStore(rows, target = "chart-revenue-store") {
    const months = [...new Set(rows.map(r => r.month))].sort();
    const stores = [...new Set(rows.map(r => r.store))];
    const palette = { "Store 1": "#f5c842", "Store 2": "#2dd4bf" };
    const chartType = chartTypeFor("monthly_revenue_per_store", "line");
    const traces = stores.map(s => {
      const values = months.map(month => {
        const found = rows.find(r => r.store === s && r.month === month);
        return Number(found?.revenue || 0);
      });
      const isBar = chartType === "bar";
      const isScatter = chartType === "scatter";
      const trace = {
        type: isBar ? "bar" : "scatter",
        name: s,
        x: months,
        y: values,
        customdata: months.map((month, idx) => [monthLabel(month), `$${values[idx].toLocaleString()}`]),
        hovertemplate: `<b>${s}</b><br>Month: %{customdata[0]}<br>Revenue: %{customdata[1]}<extra></extra>`,
        marker: { color: palette[s] || undefined },
      };
      if (isBar) {
        trace.text = values.map(value => value > 0 ? `$${value.toLocaleString()}` : "");
        trace.textposition = "outside";
        trace.cliponaxis = false;
        trace.marker.line = { color: "rgba(255,255,255,0.2)", width: 1 };
      } else {
        trace.mode = isScatter ? "markers+text" : "lines+markers+text";
        trace.text = values.map(value => value > 0 ? `$${value.toLocaleString()}` : "");
        trace.textposition = "top center";
        trace.textfont = { size: 9 };
        trace.line = { width: 2.5, color: palette[s] || undefined };
        trace.marker = { size: isScatter ? 10 : 7, color: palette[s] || undefined };
        if (chartType === "area") trace.fill = "tozeroy";
      }
      return trace;
    });
    const totalsByMonth = months.map(month =>
      rows.filter(r => r.month === month).reduce((sum, r) => sum + Number(r.revenue || 0), 0)
    );
    const peakIndex = totalsByMonth.indexOf(Math.max(...totalsByMonth));
    const chartLabel = { line: "line chart", bar: "bar chart", area: "area chart", scatter: "scatter chart" }[chartType];
    plot(target, traces, {
      title: `Monthly Revenue per Store - ${chartLabel}; compare each store month by month`,
      barmode: chartType === "bar" ? "group" : undefined,
      bargap: chartType === "bar" ? 0.34 : 0.18,
      bargroupgap: chartType === "bar" ? 0.08 : 0,
      margin: { l: 70, r: 35, t: 72, b: 88 },
      legend: { orientation: "h", x: 0, y: 1.12 },
      xaxis: {
        title: "Revenue month",
        type: "category",
        categoryorder: "array",
        categoryarray: months,
        tickvals: months,
        ticktext: months.map(monthLabel),
        tickangle: months.length > 8 ? -20 : 0,
        automargin: true,
      },
      yaxis: { title: "Revenue ($)", rangemode: "tozero", automargin: true },
      annotations: [{
        x: months[peakIndex],
        y: totalsByMonth[peakIndex],
        text: `Peak total: $${Math.round(totalsByMonth[peakIndex]).toLocaleString()}`,
        showarrow: true,
        arrowhead: 2,
        ax: 0,
        ay: -40,
        font: { size: 12 },
      }],
    });
  }

  function chartRevenueHighlight(rows, target = "chart-revenue-highlight") {
      if (!rows.length) return;
      const chartType = chartTypeFor("monthly_revenue", "bar");
      const months = rows.map(r => r.month);
      const max = rows.reduce((a, b) => a.total_revenue > b.total_revenue ? a : b);
      const min = rows.reduce((a, b) => a.total_revenue < b.total_revenue ? a : b);
      const colors = rows.map(r => {
        if (r.month === max.month) return "#f5c842";
        if (r.month === min.month) return "#f87171";
        return "#1c3055";
      });
      const isBar = chartType === "bar";
      const isScatter = chartType === "scatter";
      const trace = {
        type: isBar ? "bar" : "scatter",
        x: months,
        y: rows.map(r => r.total_revenue),
        text: rows.map(r => `$${Number(r.total_revenue).toLocaleString()}`),
        customdata: months.map(monthLabel),
        hovertemplate: "<b>%{customdata}</b><br>Revenue: $%{y:,.2f}<extra></extra>",
      };
      if (isBar) {
        trace.textposition = "outside";
        trace.cliponaxis = false;
        trace.marker = { color: colors, line: { color: "rgba(255,255,255,0.18)", width: 1 } };
      } else {
        trace.mode = isScatter ? "markers+text" : "lines+markers+text";
        trace.textposition = "top center";
        trace.textfont = { size: 9 };
        trace.marker = { size: isScatter ? 10 : 7, color: "#2dd4bf" };
        trace.line = { width: 2.5, color: "#2dd4bf" };
        if (chartType === "area") trace.fill = "tozeroy";
      }
      const chartLabel = { line: "line chart", bar: "bar chart", area: "area chart", scatter: "scatter chart" }[chartType];
      plot(target, [trace], {
        title: `Monthly Revenue - ${chartLabel}; highest month in gold, lowest in red`,
        margin: { l: 70, r: 35, t: 72, b: 88 },
        xaxis: {
          title: "Revenue month",
          type: "category",
          categoryorder: "array",
          categoryarray: months,
          tickvals: months,
          ticktext: months.map(monthLabel),
          tickangle: months.length > 8 ? -20 : 0,
          automargin: true,
        },
        yaxis: { title: "Revenue ($)", rangemode: "tozero", automargin: true },
        annotations: [
          {
            x: max.month,
            y: max.total_revenue,
            text: `Highest: $${Number(max.total_revenue).toLocaleString()}`,
            showarrow: true,
            arrowhead: 2,
            ax: 0,
            ay: -42,
            font: { size: 12 },
          },
          {
            x: min.month,
            y: min.total_revenue,
            text: `Lowest: $${Number(min.total_revenue).toLocaleString()}`,
            showarrow: true,
            arrowhead: 2,
            ax: 0,
            ay: 42,
            font: { size: 12 },
          },
        ],
      });
  }

  function chartRevenueGenre(rows, target = "chart-revenue-genre") {
    const sorted = [...rows].sort((a, b) => a.revenue - b.revenue);
    plot(target, [{
      type: "bar", orientation: "h",
      x: sorted.map(r => r.revenue),
      y: sorted.map(r => r.genre),
      text: sorted.map(r => `$${r.revenue}`),
      textposition: "outside",
      marker: {
        color: sorted.map(r => r.revenue),
        colorscale: [[0, "#1c2138"], [1, "#2dd4bf"]],
      },
    }], { title: "Revenue per Genre" });
  }

  function chartTopCustomers(rows, target = "chart-top-customers", titleOverride = "") {
      const sorted = [...rows].sort((a, b) => a.spending - b.spending);
      const title = titleOverride || `Top ${sorted.length} Customers by Spending`;
      plot(target, [{
        type: "bar", orientation: "h",
        x: sorted.map(r => r.spending),
      y: sorted.map(r => r.customer),
      text: sorted.map(r => `$${r.spending}`),
      textposition: "outside",
      marker: {
        color: sorted.map(r => r.spending),
        colorscale: [[0, "#1c2138"], [1, "#f5c842"]],
      },
      }], { title });
    }

  // ===== Render the whole dashboard =====
  function renderAll() {
    try {
      renderKPIs();
    } catch (e) {
      console.error("Failed to render KPI cards:", e);
    }

    const chartJobs = [
      ["chart-genre-bar", () => chartGenreBar(DATA.overview.genre_distribution)],
      ["chart-genre-pie", () => chartGenrePie(DATA.overview.genre_distribution)],
      ["chart-rating-pie", () => chartRatingPie(DATA.overview.rating_distribution)],
      ["chart-duration-genre", () => chartDurationGenre(DATA.overview.duration_by_genre)],
      ["chart-rental-period", () => chartRentalPeriod(DATA.overview.rental_period_distribution)],
      ["chart-genre-rating", () => chartGenreRating(DATA.overview.genre_rating_heatmap)],
      ["chart-genre-extremes", () => chartGenreExtremes(DATA.overview.genre_extremes)],

      ["chart-top-rented", () => chartTopRented(DATA.popularity.top_rented_films)],
      ["chart-monthly-rental", () => chartMonthlyRental(DATA.popularity.monthly_rental_trend)],
      ["chart-rental-dow", () => chartRentalDow(DATA.popularity.rental_by_dow)],
      ["chart-store-top", () => chartStoreTop(DATA.popularity.top_films_per_store)],
      ["chart-least-rented", () => chartLeastRented(DATA.popularity.least_rented_films)],

      ["chart-actor-films", () => chartActorFilms(DATA.actor.actor_film_count)],
      ["chart-actor-rentals", () => chartActorRentals(DATA.actor.actor_rental_count)],
      ["chart-actor-genre", () => chartActorGenre(DATA.actor.top_actor_genre_mix)],

      ["chart-revenue-store", () => chartRevenueStore(DATA.revenue.monthly_revenue_per_store)],
      ["chart-revenue-highlight", () => chartRevenueHighlight(DATA.revenue.monthly_revenue)],
      ["chart-revenue-genre", () => chartRevenueGenre(DATA.revenue.revenue_by_genre)],
      ["chart-top-customers", () => chartTopCustomers(DATA.revenue.top_customers)],
    ];

    renderLocalTransformerStatus(DATA.ml?.local_transformer || null);

    chartJobs.forEach(([target, render]) => {
      try {
        render();
      } catch (e) {
        console.error(`Failed to prepare chart ${target}:`, e);
        const el = document.getElementById(target);
        if (el) {
          el.innerHTML = `<div style="padding:1rem;color:var(--red)">Chart data failed: ${e.message}</div>`;
        }
      }
    });
  }

  // ===== Theme =====
  function setTheme(theme, options = {}) {
    const valid = ["dark", "light", "gold", "ocean", "sunset"];
    if (!valid.includes(theme)) return setBackgroundColor(theme, options);
    CURRENT_THEME = theme;
    document.body.setAttribute("data-theme", theme);
    clearCustomPalette();
    document.querySelectorAll(".theme-btn").forEach(b =>
      b.classList.toggle("active", b.dataset.theme === theme));
    if (options.persist !== false) {
      saveVisualState({ mode: "theme", theme });
    }
    // Re-draw all charts so axis/text colors update
    if (DATA) renderAll();
    return true;
  }

  // ===== Filter (popularity → top_rented_films by genre) =====
  async function filterGenre(genre) {
    const tag = document.getElementById("genre-filter-tag");
    if (!genre || genre.toLowerCase() === "all") {
      chartTopRented(DATA.popularity.top_rented_films);
      tag.style.display = "none";
      return;
    }
    try {
      const r = await fetch(`/api/dashboard`);
      // Cheap filter on existing data first
      const filtered = DATA.popularity.top_rented_films.filter(
        x => x.genre.toLowerCase() === genre.toLowerCase());
      if (filtered.length) {
        chartTopRented(filtered);
        tag.textContent = `Filter: ${genre}`;
        tag.style.display = "inline-block";
        return;
      }
      // fall through – ask AI chart endpoint via the chat result already
    } catch (e) { console.error(e); }
  }

  function scrollToSection(name) {
    const map = {
      overview: "section-overview",
      popularity: "section-popularity",
      actor: "section-actor",
      revenue: "section-revenue",
      ml: "section-ml",
    };
    const id = map[name] || name;
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });
    document.querySelectorAll(".nav-item").forEach(n =>
      n.classList.toggle("active", n.dataset.target === id));
  }

  function highlightKpi(label) {
    const key = (label || "").toLowerCase();
    const all = document.querySelectorAll(".kpi");
    all.forEach(el => el.classList.remove("highlight"));
    let match = null;
    all.forEach(el => {
      const text = el.textContent.toLowerCase();
      if (!match && text.includes(key.replace(/_/g, " "))) match = el;
    });
    if (match) {
      match.classList.add("highlight");
      match.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  }

  function outputTitle(kind, id, fallback) {
    if (fallback) return fallback;
    const labels = {
      top_rented_films: "Top Rented Films",
      top_customers: "Top Customers",
      monthly_revenue_per_store: "Monthly Revenue per Store",
      revenue_by_genre: "Revenue by Genre",
      monthly_revenue: "Monthly Revenue",
      genre_distribution: "Genre Distribution",
      rating_distribution: "Rating Distribution",
      actor_rental_count: "Actor Rental Count",
      actor_film_count: "Actor Film Count",
      least_rented_films: "Least Rented Films",
      monthly_rental_trend: "Monthly Rental Trend",
      rental_by_dow: "Rental by Day of Week",
    };
    return `${labels[id] || id || "AI Output"} ${kind === "table" ? "Table" : "Chart"}`;
  }

  function chartCustom(payload, target) {
    const spec = payload?.spec || {};
    const rows = Array.isArray(payload?.data) ? payload.data : [];
    const chartType = String(spec.chart_type || "bar").toLowerCase();
    const xLabel = spec.dimension_label || "Category";
    const yLabel = spec.metric_label || "Value";
    const seriesLabel = spec.series_label || "Series";
    const title = payload?.title || "Custom Chart";
    const isPie = chartType === "pie" || chartType === "donut";
    const isScatter = chartType === "scatter";
    const isArea = chartType === "area";
    const isBar = chartType === "bar";

    if (!rows.length) {
      const el = document.getElementById(target);
      if (el) el.innerHTML = `<div style="padding:1rem;color:var(--muted)">No data available for this custom chart.</div>`;
      return;
    }

    if (isPie) {
      plot(target, [{
        type: "pie",
        labels: rows.map(r => r.x),
        values: rows.map(r => Number(r.y || 0)),
        hole: chartType === "donut" ? 0.55 : 0,
        textinfo: "label+percent",
        hovertemplate: `<b>%{label}</b><br>${yLabel}: %{value:,}<extra></extra>`,
        marker: {
          colors: ["#2dd4bf", "#f5c842", "#fb923c", "#8b5cf6", "#60a5fa", "#f87171", "#34d399", "#fbbf24"],
        },
      }], {
        title,
        margin: { l: 40, r: 40, t: 60, b: 40 },
        showlegend: true,
      });
      return;
    }

    const grouped = {};
    rows.forEach(row => {
      const key = row.series || "__single__";
      grouped[key] = grouped[key] || [];
      grouped[key].push(row);
    });

    const traces = Object.entries(grouped).map(([seriesName, list], index) => {
      const trace = {
        type: isBar ? "bar" : "scatter",
        name: seriesName === "__single__" ? yLabel : seriesName,
        x: list.map(r => r.x),
        y: list.map(r => Number(r.y || 0)),
        text: list.map(r => formatNumber(r.y || 0)),
        customdata: list.map(r => r.series),
        hovertemplate:
          `<b>%{x}</b><br>${yLabel}: %{y:,}` +
          (seriesName === "__single__" ? "" : `<br>${seriesLabel}: %{customdata}`) +
          "<extra></extra>",
      };

      if (isBar) {
        trace.marker = {
          color: ["#f5c842", "#2dd4bf", "#8b5cf6", "#60a5fa", "#fb923c"][index % 5],
          line: { color: "rgba(255,255,255,0.2)", width: 1 },
        };
        trace.textposition = "outside";
      } else {
        trace.mode = isScatter ? "markers" : "lines+markers";
        trace.line = { width: 3, shape: "spline" };
        trace.marker = { size: isScatter ? 11 : 8 };
        if (isArea) trace.fill = "tozeroy";
      }
      return trace;
    });

    plot(target, traces, {
      title,
      barmode: Object.keys(grouped).length > 1 ? "group" : "relative",
      margin: { l: 70, r: 35, t: 60, b: 70 },
      xaxis: {
        title: xLabel,
        tickangle: rows.length > 8 ? -25 : 0,
        type: spec.dimension === "rental_month" || spec.dimension === "payment_month" ? "category" : undefined,
      },
      yaxis: { title: yLabel },
      showlegend: Object.keys(grouped).length > 1,
    });
  }

  function renderChartPayload(payload, target) {
    if (payload?.chart === "custom" || payload?.spec) {
      chartCustom(payload, target);
      return;
    }
    const map = {
      top_rented_films: chartTopRented,
      top_customers: chartTopCustomers,
      monthly_revenue_per_store: chartRevenueStore,
      revenue_by_genre: chartRevenueGenre,
      monthly_revenue: chartRevenueHighlight,
      genre_distribution: chartGenreBar,
      rating_distribution: chartRatingPie,
      actor_rental_count: chartActorRentals,
      actor_film_count: chartActorFilms,
      least_rented_films: chartLeastRented,
      monthly_rental_trend: chartMonthlyRental,
      rental_by_dow: chartRentalDow,
    };
    const fn = map[payload.chart];
    if (!fn) {
      document.getElementById(target).innerHTML =
        `<div style="padding:1rem;color:var(--red)">Unknown chart: ${payload.chart}</div>`;
      return;
    }
      fn(payload.data || [], target, payload.title || "");
    }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatCell(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
    return escapeHtml(value);
  }

  function renderTablePayload(payload, target) {
    const el = document.getElementById(target);
    if (!el) return;
    const rows = Array.isArray(payload.data) ? payload.data : [];
    if (!rows.length) {
      el.innerHTML = `<div style="padding:1rem;color:var(--muted2)">No rows returned for this table.</div>`;
      return;
    }

    const columns = Array.isArray(payload.columns) && payload.columns.length
      ? payload.columns
      : Object.keys(rows[0]);
    const head = columns.map(col => `<th>${formatCell(col).replace(/_/g, " ")}</th>`).join("");
    const body = rows.map(row => (
      `<tr>${columns.map(col => `<td>${formatCell(row[col])}</td>`).join("")}</tr>`
    )).join("");

    el.innerHTML = `
      <div class="ai-table-wrap">
        <table class="ai-table">
          <thead><tr>${head}</tr></thead>
          <tbody>${body}</tbody>
        </table>
      </div>`;
  }

  function removeAiOutput(index) {
    const idx = Number(index);
    if (!Number.isInteger(idx) || idx < 0 || idx >= AI_OUTPUTS.length) return false;
    AI_OUTPUTS = AI_OUTPUTS.filter((_, i) => i !== idx);
    renderAiOutputs();
    saveAiOutputs();
    return true;
  }

  function findAiOutputIndex(target) {
    const key = String(target || "").trim().toLowerCase();
    if (!AI_OUTPUTS.length) return -1;
    if (!key || key === "latest" || key === "last" || key === "terakhir") return AI_OUTPUTS.length - 1;
    if (key === "first" || key === "pertama") return 0;

    const numeric = Number(key);
    if (Number.isInteger(numeric) && numeric >= 0 && numeric < AI_OUTPUTS.length) return numeric;

    for (let i = AI_OUTPUTS.length - 1; i >= 0; i -= 1) {
      const item = AI_OUTPUTS[i];
      const haystack = [
        item.chart,
        item.table,
        item.title,
        outputTitle(item.type, item.chart || item.table, item.title),
      ].filter(Boolean).join(" ").toLowerCase();
      if (haystack.includes(key)) return i;
    }
    return -1;
  }

  function findAiChartIndex(target) {
    const key = String(target || "").trim().toLowerCase();
    if (!AI_OUTPUTS.length) return -1;

    if (!key || key === "latest" || key === "last" || key === "terakhir") {
      for (let i = AI_OUTPUTS.length - 1; i >= 0; i -= 1) {
        if (AI_OUTPUTS[i]?.type === "chart") return i;
      }
      return -1;
    }

    if (key === "first" || key === "pertama") {
      for (let i = 0; i < AI_OUTPUTS.length; i += 1) {
        if (AI_OUTPUTS[i]?.type === "chart") return i;
      }
      return -1;
    }

    const numeric = Number(key);
    if (Number.isInteger(numeric) && numeric >= 0 && numeric < AI_OUTPUTS.length) {
      return AI_OUTPUTS[numeric]?.type === "chart" ? numeric : -1;
    }

    for (let i = AI_OUTPUTS.length - 1; i >= 0; i -= 1) {
      const item = AI_OUTPUTS[i];
      if (item?.type !== "chart") continue;
      const haystack = [
        item.chart,
        item.title,
        outputTitle(item.type, item.chart || item.table, item.title),
      ].filter(Boolean).join(" ").toLowerCase();
      if (haystack.includes(key)) return i;
    }
    return -1;
  }

  function clearAiOutputs() {
    if (!AI_OUTPUTS.length) return false;
    AI_OUTPUTS = [];
    renderAiOutputs();
    saveAiOutputs();
    return true;
  }

  function updateAiOutput(target, changes = {}) {
    const idx = findAiChartIndex(target);
    if (idx < 0) {
      return {
        ok: false,
        reason: "I couldn't find an existing AI-generated chart to update.",
      };
    }

    const item = { ...AI_OUTPUTS[idx] };
    if (item.type !== "chart") {
      return {
        ok: false,
        reason: "The matched AI-generated output is a table, not a chart.",
      };
    }

    const nextTitle = changes.title ? String(changes.title).trim() : "";
    if (nextTitle) item.title = nextTitle;

    const nextType = normalizeChartType(changes.chart_type || changes.type);
    if (nextType) {
      if (item.chart === "custom" || item.spec) {
        item.spec = { ...(item.spec || {}), chart_type: nextType };
      } else if (item.chart) {
        if (!["line", "bar", "area", "scatter"].includes(nextType)) {
          return {
            ok: false,
            reason: `That chart can't be changed into ${nextType}. Built-in time-series charts only support bar, line, area, or scatter.`,
          };
        }
        setChartType(item.chart, nextType, { scroll: false });
      }
    } else if (!nextTitle) {
      return {
        ok: false,
        reason: "I couldn't detect a valid chart change request.",
      };
    }

    AI_OUTPUTS = AI_OUTPUTS.map((entry, i) => (i === idx ? item : entry));
    renderAiOutputs({ scroll: false });
    saveAiOutputs();
    return { ok: true, target: item.title || item.chart || `#${idx + 1}` };
  }

  function renderAiOutputs(options = {}) {
    const wrap = document.getElementById("ai-output");
    const container = document.getElementById("chart-ai");
    if (!wrap || !container) return;

    if (!AI_OUTPUTS.length) {
      wrap.style.display = "none";
      container.innerHTML = "";
      return;
    }

    wrap.style.display = "block";
    container.innerHTML = AI_OUTPUTS.map((item, index) => {
      const target = `ai-output-${index}`;
      return `
        <div class="ai-artifact" data-ai-artifact="${index}">
          <div class="ai-artifact-head">
            <b>${escapeHtml(outputTitle(item.type, item.chart || item.table, item.title))}</b>
            <div class="ai-artifact-actions">
              <span>${item.type === "table" ? "Persistent table" : "Persistent chart"}</span>
              <button type="button" class="ai-close" data-ai-remove="${index}">Close</button>
            </div>
          </div>
          <div id="${target}" class="ai-artifact-body"></div>
        </div>`;
    }).join("");

    AI_OUTPUTS.forEach((item, index) => {
      const target = `ai-output-${index}`;
      if (item.type === "table") renderTablePayload(item, target);
      else renderChartPayload(item, target);
    });

    container.querySelectorAll("[data-ai-remove]").forEach(btn => {
      btn.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        removeAiOutput(btn.dataset.aiRemove);
      });
    });

    if (options.scroll) {
      const latest = container.querySelector(`[data-ai-artifact="${AI_OUTPUTS.length - 1}"]`);
      if (latest) latest.scrollIntoView({ behavior: "smooth", block: "start" });
      else wrap.scrollIntoView({ behavior: "smooth", block: "start" });
    }
  }

  function addAiOutput(item) {
    AI_OUTPUTS = [...AI_OUTPUTS, { ...item, createdAt: Date.now() }].slice(-8);
    renderAiOutputs({ scroll: true });
    saveAiOutputs();
  }

  // Render an AI-requested chart in the dedicated AI section
  function renderAiChart(payload) {
    addAiOutput({
      type: "chart",
      chart: payload.chart,
      title: payload.title,
      spec: payload.spec,
      data: payload.data || [],
    });
  }

  function renderAiTable(payload) {
    addAiOutput({
      type: "table",
      table: payload.table || payload.chart,
      title: payload.title,
      columns: payload.columns,
      data: payload.data || [],
    });
  }

  async function reloadData(options = {}) {
    try {
      const res = await fetch("/api/dashboard", { cache: "no-store" });
      if (!res.ok) throw new Error(await res.text());
      DATA = await res.json();
      renderAll();
      renderAiOutputs({ scroll: options.scroll === true });
      return { ok: true };
    } catch (e) {
      console.error("Dashboard reload failed:", e);
      return { ok: false, reason: e.message || "Unknown dashboard reload error" };
    }
  }

  // ===== Boot =====
  async function init() {
    await restoreVisualState();
    CHART_TYPES = readChartTypes();

    try {
      const result = await reloadData();
      if (!result.ok) throw new Error(result.reason || "Unable to load dashboard");
      await restoreAiOutputs();
    } catch (e) {
      console.error("Dashboard load failed:", e);
      document.querySelector(".main").insertAdjacentHTML("afterbegin",
        `<div class="card" style="color:var(--red)">⚠️ Failed to load dashboard data: ${e.message}</div>`);
      return;
    }

    // Sidebar nav clicks
    document.querySelectorAll(".nav-item").forEach(item => {
      item.addEventListener("click", () => {
        const target = item.dataset.target;
        document.getElementById(target)?.scrollIntoView({ behavior: "smooth", block: "start" });
        document.querySelectorAll(".nav-item").forEach(n => n.classList.remove("active"));
        item.classList.add("active");
      });
    });

    // Theme buttons
    document.querySelectorAll(".theme-btn").forEach(btn => {
      btn.addEventListener("click", () => setTheme(btn.dataset.theme));
    });

    document.getElementById("ml-transformer-run")?.addEventListener("click", runLocalTransformerInference);
    document.getElementById("ml-forecast-run")?.addEventListener("click", runTimeSeriesForecast);
    runTimeSeriesForecast();

  }

  document.addEventListener("DOMContentLoaded", init);

  // Public API for the chat module
  return {
    setTheme, setBackgroundColor, setChartType, filterGenre, scrollToSection, highlightKpi, renderAiChart, renderAiTable,
    removeAiOutput, clearAiOutputs, updateAiOutput,
    reloadData,
    getData: () => DATA,
  };
})();

window.Dashboard = Dashboard;
