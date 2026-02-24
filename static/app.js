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

    if (payload.query_executed && payload.sql) {
      const label = document.createElement("div");
      label.textContent = `Executed SQL (${payload.row_count || 0} rows):`;
      meta.appendChild(label);

      const pre = document.createElement("pre");
      pre.textContent = payload.sql;
      meta.appendChild(pre);

      if (Array.isArray(payload.rows) && payload.rows.length > 0) {
        const tableWrap = document.createElement("div");
        tableWrap.className = "table-wrap";
        tableWrap.appendChild(renderTable(payload.rows));
        meta.appendChild(tableWrap);
      }
    }

    if (payload.proposed_schema) {
      const schemaLabel = document.createElement("div");
      schemaLabel.textContent = "Proposed schema:";
      meta.appendChild(schemaLabel);

      const schemaPre = document.createElement("pre");
      schemaPre.textContent = JSON.stringify(payload.proposed_schema, null, 2);
      meta.appendChild(schemaPre);
    }

    if (payload.schema_execution) {
      const exec = payload.schema_execution;
      const execSummary = document.createElement("div");
      const status = exec.success ? "success" : "failed";
      execSummary.textContent =
        `Schema execution (${exec.mode || "dry_run"}): ${status}, ` +
        `${exec.statement_count || 0} statements`;
      meta.appendChild(execSummary);

      if (Array.isArray(exec.statements) && exec.statements.length > 0) {
        const sqlPre = document.createElement("pre");
        sqlPre.textContent = exec.statements.join(";\n") + ";";
        meta.appendChild(sqlPre);
      }

      if (exec.error) {
        const execError = document.createElement("div");
        execError.textContent = `Execution error: ${exec.error}`;
        meta.appendChild(execError);
      }
    }

    if (payload.error && !(payload.schema_execution && payload.schema_execution.error)) {
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
