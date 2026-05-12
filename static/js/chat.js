/* =============================================================
   Floating Chat — talks to /api/chat, executes returned actions
   on the dashboard via window.Dashboard.*
   ============================================================= */

(() => {
  const fab = document.getElementById("chat-fab");
  const win = document.getElementById("chat-window");
  const msgs = document.getElementById("chat-messages");
  const input = document.getElementById("chat-input");
  const sendBtn = document.getElementById("chat-send");
  const minBtn = document.getElementById("chat-min");
  const clearBtn = document.getElementById("chat-clear");
  const suggest = document.getElementById("chat-suggest");

  let history = [];
  let busy = false;
  let awaitingBackgroundColor = false;
  let recentBackgroundCommand = false;

  // ---------- UI helpers ----------
  function open() { win.classList.add("open"); input.focus(); }
  function close() { win.classList.remove("open"); }

  function escapeHtml(text) {
    return String(text)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function renderInlineMarkdown(text) {
    let html = escapeHtml(text);

    html = html.replace(/`([^`]+)`/g, "<code>$1</code>");
    html = html.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
    html = html.replace(/(^|[\s(])\*([^*\n]+)\*(?=[\s).,!?;:]|$)/g, "$1<em>$2</em>");
    html = html.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" target="_blank" rel="noreferrer noopener">$1</a>');

    return html;
  }

  function splitTableRow(line) {
    let row = line.trim();
    if (row.startsWith("|")) row = row.slice(1);
    if (row.endsWith("|")) row = row.slice(0, -1);
    return row.split("|").map(cell => renderInlineMarkdown(cell.trim()));
  }

  function isTableSeparator(line) {
    const row = line.trim();
    if (!row.includes("|")) return false;
    const normalized = row.replace(/\s/g, "");
    return /^\|?[:\-|]+\|?$/.test(normalized) && normalized.includes("-");
  }

  function renderMarkdown(markdown) {
    const lines = String(markdown || "").replace(/\r\n/g, "\n").split("\n");
    const blocks = [];
    let i = 0;

    while (i < lines.length) {
      const line = lines[i];
      const trimmed = line.trim();

      if (!trimmed) {
        i += 1;
        continue;
      }

      if (trimmed.startsWith("```")) {
        const lang = trimmed.slice(3).trim();
        const codeLines = [];
        i += 1;
        while (i < lines.length && !lines[i].trim().startsWith("```")) {
          codeLines.push(lines[i]);
          i += 1;
        }
        if (i < lines.length) i += 1;
        const langAttr = lang ? ` data-lang="${escapeHtml(lang)}"` : "";
        blocks.push(
          `<pre><code${langAttr}>${escapeHtml(codeLines.join("\n"))}</code></pre>`
        );
        continue;
      }

      if (
        trimmed.includes("|") &&
        i + 1 < lines.length &&
        isTableSeparator(lines[i + 1])
      ) {
        const headerCells = splitTableRow(trimmed);
        const bodyRows = [];
        i += 2;

        while (i < lines.length) {
          const bodyLine = lines[i].trim();
          if (!bodyLine || !bodyLine.includes("|")) break;
          const cells = splitTableRow(bodyLine);
          if (cells.length !== headerCells.length) break;
          bodyRows.push(cells);
          i += 1;
        }

        const thead = `<thead><tr>${headerCells.map(cell => `<th>${cell}</th>`).join("")}</tr></thead>`;
        const tbody = bodyRows.length
          ? `<tbody>${bodyRows.map(row => `<tr>${row.map(cell => `<td>${cell}</td>`).join("")}</tr>`).join("")}</tbody>`
          : "";
        blocks.push(`<div class="md-table-wrap"><table>${thead}${tbody}</table></div>`);
        continue;
      }

      if (/^[-*+]\s+/.test(trimmed)) {
        const items = [];
        while (i < lines.length && /^[-*+]\s+/.test(lines[i].trim())) {
          items.push(`<li>${renderInlineMarkdown(lines[i].trim().replace(/^[-*+]\s+/, ""))}</li>`);
          i += 1;
        }
        blocks.push(`<ul>${items.join("")}</ul>`);
        continue;
      }

      if (/^\d+\.\s+/.test(trimmed)) {
        const items = [];
        while (i < lines.length && /^\d+\.\s+/.test(lines[i].trim())) {
          items.push(`<li>${renderInlineMarkdown(lines[i].trim().replace(/^\d+\.\s+/, ""))}</li>`);
          i += 1;
        }
        blocks.push(`<ol>${items.join("")}</ol>`);
        continue;
      }

      if (/^#{1,6}\s+/.test(trimmed)) {
        const level = Math.min(trimmed.match(/^#+/)[0].length, 6);
        const content = trimmed.replace(/^#{1,6}\s+/, "");
        blocks.push(`<h${level}>${renderInlineMarkdown(content)}</h${level}>`);
        i += 1;
        continue;
      }

      if (/^>\s+/.test(trimmed)) {
        const quoteLines = [];
        while (i < lines.length && /^>\s+/.test(lines[i].trim())) {
          quoteLines.push(renderInlineMarkdown(lines[i].trim().replace(/^>\s+/, "")));
          i += 1;
        }
        blocks.push(`<blockquote>${quoteLines.join("<br>")}</blockquote>`);
        continue;
      }

      const paragraph = [];
      while (i < lines.length && lines[i].trim()) {
        paragraph.push(renderInlineMarkdown(lines[i].trim()));
        i += 1;
      }
      blocks.push(`<p>${paragraph.join("<br>")}</p>`);
    }

    return blocks.join("");
  }

  function pushMessage(role, content) {
    const div = document.createElement("div");
    div.className = `msg ${role}`;
    if (role === "assistant") {
      div.classList.add("markdown");
      div.innerHTML = renderMarkdown(content);
    } else {
      div.textContent = content;
    }
    msgs.appendChild(div);
    msgs.scrollTop = msgs.scrollHeight;
    return div;
  }

  function pushAction(text) {
    const div = document.createElement("div");
    div.className = "msg action-tag";
    div.textContent = `⚡ ${text}`;
    msgs.appendChild(div);
    msgs.scrollTop = msgs.scrollHeight;
  }

  function pushTyping() {
    const div = document.createElement("div");
    div.className = "msg typing";
    div.innerHTML = `<span class="typing-dots"><span></span><span></span><span></span></span>`;
    msgs.appendChild(div);
    msgs.scrollTop = msgs.scrollHeight;
    return div;
  }

  function cleanColorCandidate(value) {
    return String(value || "")
      .replace(/[.?!,\u3002\uff0c\uff01\uff1f].*$/, "")
      .replace(/\b(?:please|pls|now|thanks|thank you|dan|lalu|terus|kemudian)\b.*$/i, "")
      .replace(/^[\s"'`]*(?:the|a|an|color|colour|warna)\b[\s,:"'`-]*/i, "")
      .replace(/^[\s"'`]+|[\s"'`.?!,\u3002\uff0c\uff01\uff1f]+$/g, "")
      .trim();
  }

  function extractExplicitColor(text) {
    const raw = String(text || "").trim();
    if (!raw) return null;

    const cssFunction = raw.match(/\b(?:rgb|rgba|hsl|hsla)\([^)]+\)/i);
    if (cssFunction) return cssFunction[0];

    const hex = raw.match(/#[0-9a-f]{3,8}\b/i);
    if (hex) return hex[0];

    const phrasePatterns = [
      /\b(?:to|into|as|with)\s+([a-z][a-z\s-]{1,32})/i,
      /\b(?:jadi|menjadi|ke|dengan|pakai|gunakan)\s+([a-z][a-z\s-]{1,32})/i,
      /(?:\u6539\u6210|\u6539\u4e3a|\u53d8\u6210|\u8bbe\u7f6e\u6210|\u8bbe\u7f6e\u4e3a|\u8bbe\u6210|\u8bbe\u4e3a|\u6362\u6210|\u6362\u4e3a|\u8c03\u6210|\u8c03\u4e3a|\u8c03\u6574\u6210|\u8c03\u6574\u4e3a|\u5f04\u6210|\u5f04\u4e3a)\s*([\u4e00-\u9fa5a-zA-Z\s-]{1,32})/i,
      /(?:\u80cc\u666f|\u80cc\u666f\u8272|\u989c\u8272|\u7f51\u7ad9|\u9875\u9762).*?(?:\u4e3a|\u6210|\u5230)\s*([\u4e00-\u9fa5a-zA-Z\s-]{1,32})/i,
    ];

    for (const pattern of phrasePatterns) {
      const match = raw.match(pattern);
      if (match) {
        const color = cleanColorCandidate(match[1]);
        if (color) return color;
      }
    }

    return null;
  }

  function extractBackgroundColor(text) {
    const raw = String(text || "").trim();
    if (!raw) return null;

    const looksLikeBackgroundCommand =
      /\b(background|bg|page|site|website|dashboard|color|colour)\b/i.test(raw) ||
      /[\u80cc\u666f\u989c\u8272\u7f51\u7ad9\u9875\u9762]/.test(raw) ||
      /\b(latar|warna|halaman|situs)\b/i.test(raw);

    const asksToChange =
      /\b(change|switch|set|make|turn|apply|adjust|update|use|paint|choose|ubah|ganti|jadikan|buat|atur|setel|terapkan|gunakan|pilih)\b/i.test(raw) ||
      /[\u6539\u6362\u53d8\u8bbe\u5207\u8c03\u5f04]/.test(raw);

    if (!looksLikeBackgroundCommand || !asksToChange) return null;

    const explicitColor = extractExplicitColor(raw);
    if (explicitColor) return explicitColor;

    const words = raw.replace(/[.?!,\u3002\uff0c\uff01\uff1f]/g, " ").trim().split(/\s+/);
    return words.length ? words[words.length - 1] : null;
  }

  function extractStandaloneColor(text) {
    const raw = String(text || "").trim();
    if (!raw) return null;

    const explicitColor = extractExplicitColor(raw);
    if (explicitColor) return explicitColor;

    const cleaned = cleanColorCandidate(raw
      .replace(/^[\s"'`]*(?:ok|okay|yes|yeah|sure|please|pls|use|try|set it to|change it to|make it into|make it to|make it|turn it to|turn it|adjust it to|update it to|paint it|gunakan|pakai|coba|ubah ke|ganti ke|jadikan|buat jadi|atur ke|setel ke|terapkan)\b[\s,:"'`-]*/i, "")
    );

    if (/^[a-z][a-z\s-]{1,32}$/i.test(cleaned)) return cleaned;
    if (/^[\u4e00-\u9fa5]{1,12}$/.test(cleaned)) return cleaned;
    return null;
  }

  function expectsBackgroundColorReply(text) {
    const raw = String(text || "");
    const asksForAnotherColor =
      /\b(background|bg|page|site|website|dashboard|color|colour)\b/i.test(raw) &&
      /\b(another|other|different|choose|what color|which color)\b/i.test(raw);
    const asksInIndonesian =
      /\b(latar|warna|halaman|situs|background)\b/i.test(raw) &&
      /\b(lain|berbeda|pilih|apa|warna apa|yang mana)\b/i.test(raw);
    const asksInChinese =
      /[\u80cc\u666f\u989c\u8272]/.test(raw) &&
      /[\u6362\u6539\u8c03\u5f04\u9009\u54ea\u4ec0\u4e48]/.test(raw);
    return asksForAnotherColor || asksInIndonesian || asksInChinese;
  }

  function backgroundChangedReply(text, color) {
    const raw = String(text || "");
    if (/[\u4e00-\u9fff]/.test(raw)) {
      return `背景已改成 ${color}，并且会保存下来。`;
    }
    if (/\b(latar|warna|halaman|situs|ubah|ganti|jadikan|atur|setel|terapkan|gunakan)\b/i.test(raw)) {
      return `Latar belakang sudah diubah ke ${color} dan akan tersimpan.`;
    }
    return `Background changed to ${color} and saved.`;
  }

  function isColorOnlyCommand(text) {
    const raw = String(text || "");
    const hasExtraDashboardRequest =
      /\b(top|customer|customers|film|films|movie|movies|revenue|chart|render|show|display|filter|scroll|section|kpi|actor|actors|genre|rental|rentals)\b/i.test(raw) ||
      /\b(tampilkan|pelanggan|pendapatan|grafik|aktor|genre|sewa|filter|bagian)\b/i.test(raw) ||
      /[\u56fe\u8868\u663e\u793a\u5c55\u793a\u7b5b\u9009\u8fc7\u6ee4\u6536\u5165\u8425\u6536\u5ba2\u6237\u987e\u5ba2\u6f14\u5458\u7535\u5f71]/.test(raw);
    const chainsAnotherRequest =
      /\b(and|then|also|dan|lalu|terus|kemudian)\b/i.test(raw) ||
      /[\u5e76\u4e14\u7136\u540e\u987a\u4fbf\u540c\u65f6]/.test(raw);
    return !(hasExtraDashboardRequest || chainsAnotherRequest);
  }

  function welcome() {
    pushMessage("assistant",
      "Halo! Saya AI assistant DVD Rental Dashboard. Saya bisa:\n" +
      "• Menjawab pertanyaan tentang film, customer, revenue, actor\n" +
      "• Mengganti tema (dark/light/gold/ocean/sunset)\n" +
      "• Menampilkan chart atau tabel sesuai permintaan (top film, top customer, dll)\n" +
      "• Filter & scroll ke section tertentu\n" +
      "• Menjawab hanya hal yang terkait dashboard DVD Rental\n\n" +
      "Coba klik chip di bawah, atau ketik pertanyaan dashboard kamu 👇");
  }

  // ---------- Action executor ----------
  function welcome() {
    pushMessage("assistant",
      "Halo! Saya AI assistant DVD Rental Dashboard. Saya bisa:\n" +
      "- Menjawab pertanyaan tentang film, customer, revenue, actor\n" +
      "- Mengganti tema (dark/light/gold/ocean/sunset)\n" +
      "- Menampilkan chart atau tabel sesuai permintaan (top film, top customer, revenue, actor, dll)\n" +
      "- Membuat chart langsung dari chat kamu, tidak hanya dari chip yang tersedia\n" +
      "- Filter & scroll ke section tertentu\n" +
      "- Menjawab hanya hal yang terkait dashboard DVD Rental\n\n" +
      "Coba klik chip di bawah, atau ketik langsung misalnya: buat grafik revenue per genre, tampilkan top 10 film sebagai tabel, atau ubah monthly revenue jadi bar chart.");
  }

  async function runActions(actions) {
    if (!Array.isArray(actions) || !actions.length) return;
    const D = window.Dashboard;
    if (!D) return;

    for (const act of actions) {
      if (!act || typeof act !== "object") return;
      const typeMap = {
        set_bg: "set_background",
        change_background: "set_background",
        background: "set_background",
        set_background_color: "set_background",
        change_background_color: "set_background",
      };
      const type = typeMap[act.type] || act.type;

      switch (type) {
        case "set_theme":
          if (D.setTheme(act.theme)) pushAction(`Theme switched to ${act.theme}`);
          break;
        case "set_background":
          {
            const color = act.color || act.theme || act.background || act.value;
            if (D.setBackgroundColor && D.setBackgroundColor(color)) {
              pushAction(`Background changed to ${color}`);
              recentBackgroundCommand = true;
            }
          }
          break;
        case "scroll_to":
          D.scrollToSection(act.section);
          pushAction(`Scrolled to ${act.section}`);
          break;
        case "render_chart":
          if (act.error) pushAction(`Chart error: ${act.error}`);
          else if ("data" in act) {
            D.renderAiChart(act);
            pushAction(`Rendered: ${act.chart}`);
          }
          break;
        case "render_custom_chart":
          if (act.error) pushAction(`Custom chart error: ${act.error}`);
          else if ("data" in act) {
            D.renderAiChart(act);
            pushAction(`Rendered custom chart: ${act.title || "custom"}`);
          }
          break;
        case "render_table":
          if (act.error) pushAction(`Table error: ${act.error}`);
          else if ("data" in act) {
            D.renderAiTable(act);
            pushAction(`Rendered table: ${act.table || act.chart}`);
          }
          break;
        case "query_records":
          if (act.ok === false || act.error) {
            pushAction(act.error || "Database query failed");
          } else if ("data" in act) {
            D.renderAiTable({
              type: "table",
              table: act.table,
              title: act.title || `Query: ${act.table}`,
              columns: act.columns,
              data: act.data || [],
            });
            pushAction(act.summary || `Rendered query result: ${act.table}`);
          }
          break;
        case "delete_ai_output":
          {
            const target = act.target || act.chart || act.title || "latest";
            if (D.removeAiOutput?.(target)) pushAction(`Removed chart/table: ${target}`);
            else pushAction(`Nothing matched for removal: ${target}`);
          }
          break;
        case "clear_ai_outputs":
          if (D.clearAiOutputs?.()) pushAction("Removed all AI-generated charts/tables");
          else pushAction("No AI-generated charts/tables to remove");
          break;
        case "update_ai_chart":
          {
            const target = act.target || act.chart || act.title || "latest";
            const result = D.updateAiOutput?.(target, act);
            if (result?.ok || result === true) {
              pushAction(`Updated chart: ${result?.target || target}`);
            } else {
              pushAction(result?.reason || `Chart update failed: ${target}`);
            }
          }
          break;
        case "mutate_records":
          if (act.ok) {
            pushAction(act.summary || `Database updated: ${act.table}`);
            const refreshed = await D.reloadData?.();
            if (refreshed?.ok === false) {
              pushAction(`Database changed, but dashboard refresh failed: ${refreshed.reason}`);
            } else {
              pushAction("Dashboard data refreshed");
            }
          } else {
            pushAction(act.error || "Database update failed");
          }
          break;
        case "set_chart_type":
            {
              const chart = act.chart || act.target || act.id || "monthly_revenue_per_store";
              const chartType = act.chart_type || act.type_value || act.value || act.mode;
              if (D.setChartType?.(chart, chartType)) {
              pushAction(`Chart ${chart} switched to ${chartType}`);
            } else {
              pushAction(`Chart type change failed: ${chart} -> ${chartType}`);
            }
          }
          break;
        case "filter_genre":
          D.filterGenre(act.genre);
          pushAction(`Filter applied: genre = ${act.genre}`);
          break;
        case "highlight_kpi":
          D.highlightKpi(act.kpi);
          pushAction(`Highlighted KPI: ${act.kpi}`);
          break;
        default:
          pushAction(`Unknown action: ${act.type}`);
      }
    }
  }

  // ---------- Send ----------
  async function send(text) {
    if (busy || !text.trim()) return;
    busy = true;
    sendBtn.disabled = true;

    pushMessage("user", text);
    history.push({ role: "user", content: text });
    input.value = "";
    input.style.height = "auto";

    const wasAwaitingBackgroundColor = awaitingBackgroundColor;
    const localBackgroundColor =
      extractBackgroundColor(text) ||
      ((wasAwaitingBackgroundColor || recentBackgroundCommand) ? extractStandaloneColor(text) : null);

    if (localBackgroundColor && window.Dashboard?.setBackgroundColor?.(localBackgroundColor)) {
      pushAction(`Background changed to ${localBackgroundColor}`);
      awaitingBackgroundColor = false;
      recentBackgroundCommand = true;
      if (isColorOnlyCommand(text)) {
        const reply = backgroundChangedReply(text, localBackgroundColor);
        pushMessage("assistant", reply);
        history.push({ role: "assistant", content: reply });
        busy = false;
        sendBtn.disabled = false;
        input.focus();
        return;
      }
    } else if (expectsBackgroundColorReply(text)) {
      awaitingBackgroundColor = true;
    } else {
      recentBackgroundCommand = false;
    }

    const typingEl = pushTyping();

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: history }),
      });
      typingEl.remove();
      if (!res.ok) {
        const err = await res.text();
        pushMessage("assistant", `⚠️ Server error: ${err}`);
        return;
      }
      const data = await res.json();
      const reply = (data.reply || "").trim();
      if (reply) {
        pushMessage("assistant", reply);
        history.push({ role: "assistant", content: reply });
        if (/what color would you like|choose any css color|which color/i.test(reply)) {
          awaitingBackgroundColor = true;
        }
      }
      await runActions(data.actions || []);
    } catch (e) {
      typingEl.remove();
      pushMessage("assistant", `⚠️ Network error: ${e.message}`);
    } finally {
      busy = false;
      sendBtn.disabled = false;
      input.focus();
    }
  }

  // ---------- Wire up ----------
  fab.addEventListener("click", () => {
    if (win.classList.contains("open")) close();
    else open();
  });
  minBtn.addEventListener("click", close);

  clearBtn.addEventListener("click", () => {
    history = [];
    msgs.innerHTML = "";
    welcome();
  });

  sendBtn.addEventListener("click", () => send(input.value));
  input.addEventListener("keydown", e => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send(input.value);
    }
  });
  input.addEventListener("input", () => {
    input.style.height = "auto";
    input.style.height = Math.min(input.scrollHeight, 120) + "px";
  });

  suggest.querySelectorAll(".chip").forEach(chip => {
    chip.addEventListener("click", () => {
      const q = chip.dataset.q;
      send(q);
    });
  });

  // Initial welcome
  welcome();
})();
