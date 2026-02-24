const chatLog = document.getElementById("chat-log");
const chatForm = document.getElementById("chat-form");
const chatInput = document.getElementById("chat-input");
const sendBtn = document.getElementById("send-btn");

const history = [];

function autoResizeInput() {
  chatInput.style.height = "auto";
  chatInput.style.height = `${Math.min(chatInput.scrollHeight, 180)}px`;
}

function scrollToBottom() {
  chatLog.scrollTop = chatLog.scrollHeight;
}

function appendMessage(role, text, payload = null) {
  const message = document.createElement("article");
  message.className = `message ${role}`;

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.textContent = text;
  message.appendChild(bubble);

  if (role === "assistant" && payload) {
    const meta = document.createElement("div");
    meta.className = "meta";

    if (payload.query_plan && payload.query_plan.sql) {
      const label = document.createElement("div");
      label.textContent = "Query plan SQL:";
      meta.appendChild(label);

      const pre = document.createElement("pre");
      const params = Array.isArray(payload.params) ? payload.params : [];
      pre.textContent = `${payload.query_plan.sql}\n\nparams: ${JSON.stringify(params)}`;
      meta.appendChild(pre);
    }

    if (payload.agent_status) {
      const status = payload.agent_status;
      const statusLine = document.createElement("div");
      const source = status.source || "unknown";
      const cacheAge =
        typeof status.cache_age_seconds === "number"
          ? `${Math.round(status.cache_age_seconds)}s`
          : "n/a";
      statusLine.textContent = `Schema source: ${source} (cache age: ${cacheAge})`;
      meta.appendChild(statusLine);
      if (status.last_error) {
        const statusError = document.createElement("div");
        statusError.textContent = `Schema note: ${status.last_error}`;
        meta.appendChild(statusError);
      }
    }

    if (payload.display) {
      const title = document.createElement("div");
      title.textContent = payload.display.title || "Results";
      meta.appendChild(title);

      if (Array.isArray(payload.display.rows) && payload.display.rows.length > 0) {
        const tableWrap = document.createElement("div");
        tableWrap.className = "table-wrap";
        tableWrap.appendChild(renderDisplayTable(payload.display));
        meta.appendChild(tableWrap);
      } else if (payload.query_executed) {
        const empty = document.createElement("div");
        empty.textContent = "No rows returned.";
        meta.appendChild(empty);
      }
    } else if (payload.query_executed && Array.isArray(payload.rows) && payload.rows.length > 0) {
      const tableWrap = document.createElement("div");
      tableWrap.className = "table-wrap";
      tableWrap.appendChild(renderTable(payload.rows));
      meta.appendChild(tableWrap);
    }

    if (payload.error) {
      const error = document.createElement("div");
      error.textContent = `Error: ${payload.error}`;
      meta.appendChild(error);
    }

    if (meta.childElementCount > 0) {
      bubble.appendChild(meta);
    }
  }

  chatLog.appendChild(message);
  scrollToBottom();
}

function renderDisplayTable(display) {
  const table = document.createElement("table");
  const columns = Array.isArray(display.columns) ? display.columns : [];
  const rows = Array.isArray(display.rows) ? display.rows : [];

  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column.label || column.name;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      const value = row[column.name];
      td.textContent = value === null || value === undefined ? "" : String(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  return table;
}

function renderTable(rows) {
  const table = document.createElement("table");
  const columns = Object.keys(rows[0]);

  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  columns.forEach((column) => {
    const th = document.createElement("th");
    th.textContent = column;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    columns.forEach((column) => {
      const td = document.createElement("td");
      const value = row[column];
      td.textContent = value === null || value === undefined ? "" : String(value);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);

  return table;
}

function setSubmitting(isSubmitting) {
  sendBtn.disabled = isSubmitting;
  chatInput.disabled = isSubmitting;
}

async function handleSubmit(event) {
  event.preventDefault();
  const message = chatInput.value.trim();
  if (!message) {
    return;
  }

  appendMessage("user", message);
  history.push({ role: "user", content: message });
  chatInput.value = "";
  autoResizeInput();
  setSubmitting(true);

  appendMessage("assistant", "Thinking...");
  const loadingNode = chatLog.lastElementChild;

  try {
    const response = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, history }),
    });

    const payload = await response.json();
    loadingNode.remove();

    if (!response.ok) {
      const detail = payload?.detail || "Request failed.";
      appendMessage("assistant", `Could not process request: ${detail}`);
      return;
    }

    const assistantMessage = payload.assistant_message || "No response.";
    appendMessage("assistant", assistantMessage, payload);
    history.push({ role: "assistant", content: assistantMessage });
  } catch (error) {
    loadingNode.remove();
    appendMessage("assistant", `Request failed: ${error.message}`);
  } finally {
    setSubmitting(false);
    chatInput.focus();
  }
}

chatInput.addEventListener("input", autoResizeInput);
chatForm.addEventListener("submit", handleSubmit);
autoResizeInput();
