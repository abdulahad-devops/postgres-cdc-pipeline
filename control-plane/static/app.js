const state = { csrf: "", connections: [], connection: null, tables: [], table: null, view: "source" };

const $ = (id) => document.getElementById(id);
const auth = $("auth");
const workspace = $("workspace");

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (state.csrf && options.method && options.method !== "GET") headers["X-CSRF-Token"] = state.csrf;
  const response = await fetch(path, { credentials: "same-origin", ...options, headers });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.detail || `Request failed (${response.status})`);
  return payload;
}

function flash(message, error = false) {
  const node = $("flash");
  node.textContent = message || "";
  node.className = message ? `show ${error ? "error" : ""}` : "";
}

function formJson(form) {
  return Object.fromEntries(new FormData(form).entries());
}

async function boot() {
  try {
    const me = await api("/api/me");
    state.csrf = me.csrf_token;
    auth.classList.add("hidden");
    workspace.classList.remove("hidden");
    $("identity").classList.remove("hidden");
    $("who").textContent = me.email;
    $("org-name").textContent = me.organization_name;
    await loadConnections();
  } catch {
    auth.classList.remove("hidden");
  }
}

async function authenticate(path, form) {
  try {
    const result = await api(path, { method: "POST", body: JSON.stringify(formJson(form)) });
    state.csrf = result.csrf_token;
    await boot();
  } catch (error) {
    alert(error.message);
  }
}

$("login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  authenticate("/api/login", event.currentTarget);
});
$("register-form").addEventListener("submit", (event) => {
  event.preventDefault();
  authenticate("/api/register", event.currentTarget);
});
$("logout").addEventListener("click", async () => {
  await api("/api/logout", { method: "POST", body: "{}" });
  location.reload();
});

$("new-connection").addEventListener("click", () => $("connection-form-wrap").classList.remove("hidden"));
$("cancel-connection").addEventListener("click", () => $("connection-form-wrap").classList.add("hidden"));
$("connection-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = formJson(event.currentTarget);
  data.port = Number(data.port);
  try {
    flash("Testing PostgreSQL and saving encrypted credentials…");
    await api("/api/connections", { method: "POST", body: JSON.stringify(data) });
    event.currentTarget.reset();
    event.currentTarget.port.value = 5432;
    $("connection-form-wrap").classList.add("hidden");
    flash("Database connected. Select tables to begin CDC.");
    await loadConnections();
  } catch (error) {
    flash(error.message, true);
  }
});

async function loadConnections() {
  state.connections = await api("/api/connections");
  const list = $("connections");
  list.replaceChildren();
  if (!state.connections.length) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "No database connected yet.";
    list.append(p);
    return;
  }
  state.connections.forEach((connection) => {
    const item = document.createElement("div");
    item.className = "connection-item" + (state.connection?.id === connection.id ? " active" : "");
    const name = document.createElement("strong");
    name.textContent = connection.name;
    const target = document.createElement("span");
    target.textContent = `${connection.database_name} · ${connection.host}`;
    item.append(name, target);
    item.addEventListener("click", () => openConnection(connection));
    list.append(item);
  });
}

async function openConnection(connection) {
  state.connection = connection;
  state.table = null;
  $("empty-state").classList.add("hidden");
  $("connection-detail").classList.remove("hidden");
  $("data-area").classList.add("hidden");
  $("connection-name").textContent = connection.name;
  $("connection-target").textContent = `${connection.database_user}@${connection.host}:${connection.port}/${connection.database_name}`;
  await loadConnections();
  await Promise.all([loadTables(), loadHealth()]);
}

async function loadHealth() {
  if (!state.connection) return;
  try {
    const health = await api(`/api/connections/${state.connection.id}/status`);
    const metrics = [
      ["Connector", health.connector, health.connector === "RUNNING"],
      ["Task", health.task, health.task === "RUNNING"],
      ["Active mirror", health.active_records ?? 0, true],
      ["Protected deletes", health.protected_deletes ?? 0, true],
    ];
    const grid = $("health");
    grid.replaceChildren();
    metrics.forEach(([label, value, good]) => {
      const box = document.createElement("div");
      box.className = `metric ${good ? "good" : "bad"}`;
      const caption = document.createElement("span");
      caption.textContent = label;
      const strong = document.createElement("strong");
      strong.textContent = value;
      box.append(caption, strong);
      grid.append(box);
    });
  } catch (error) {
    flash(error.message, true);
  }
}

async function loadTables() {
  if (!state.connection) return;
  try {
    state.tables = await api(`/api/connections/${state.connection.id}/tables`);
    const picker = $("tables");
    picker.replaceChildren();
    state.tables.forEach((table) => {
      const label = document.createElement("label");
      label.className = "table-choice" + (table.primary_key_columns.length ? "" : " no-pk");
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.checked = table.selected;
      checkbox.disabled = !table.primary_key_columns.length;
      checkbox.dataset.schema = table.schema_name;
      checkbox.dataset.table = table.table_name;
      const title = document.createElement("span");
      title.textContent = `${table.schema_name}.${table.table_name}${table.primary_key_columns.length ? "" : " (no primary key)"}`;
      if (table.selected) title.classList.add("enabled");
      title.addEventListener("click", (event) => {
        if (table.selected) {
          event.preventDefault();
          state.table = table;
          showRows();
        }
      });
      label.append(checkbox, title);
      picker.append(label);
    });
  } catch (error) {
    flash(error.message, true);
  }
}

$("save-tables").addEventListener("click", async () => {
  const selected = [...document.querySelectorAll("#tables input:checked")].map((node) => ({
    schema_name: node.dataset.schema, table_name: node.dataset.table,
  }));
  if (!selected.length) return flash("Choose at least one table with a primary key.", true);
  try {
    flash("Creating a tenant-isolated Debezium connector and initial snapshot…");
    await api(`/api/connections/${state.connection.id}/tables`, {
      method: "PUT", body: JSON.stringify({ tables: selected }),
    });
    flash("CDC is running. Click an enabled table to manage its rows.");
    await Promise.all([loadTables(), loadHealth()]);
  } catch (error) {
    flash(error.message, true);
  }
});

async function showRows() {
  $("data-area").classList.remove("hidden");
  $("table-title").textContent = `${state.table.schema_name}.${state.table.table_name}`;
  await loadRows();
}

async function loadRows() {
  if (!state.table) return;
  const path = `/api/connections/${state.connection.id}/rows/${encodeURIComponent(state.table.schema_name)}/${encodeURIComponent(state.table.table_name)}?view=${state.view}`;
  try {
    const data = await api(path);
    renderRows(data.rows || []);
  } catch (error) {
    flash(error.message, true);
  }
}

function renderRows(rows) {
  const table = $("rows");
  table.replaceChildren();
  if (!rows.length) {
    const caption = document.createElement("caption");
    caption.textContent = "No rows found.";
    table.append(caption);
    return;
  }
  const normalized = state.view === "replica"
    ? rows.map((row) => ({ ...row.record_data, _deleted: row.is_deleted, _replicated_at: row.replicated_at }))
    : rows;
  const columns = [...new Set(normalized.flatMap((row) => Object.keys(row)))];
  const head = document.createElement("thead");
  const header = document.createElement("tr");
  [...columns, "Actions"].forEach((name) => {
    const th = document.createElement("th");
    th.textContent = name;
    header.append(th);
  });
  head.append(header);
  const body = document.createElement("tbody");
  normalized.forEach((row, index) => {
    const tr = document.createElement("tr");
    columns.forEach((name) => {
      const td = document.createElement("td");
      const value = row[name];
      td.textContent = typeof value === "object" ? JSON.stringify(value) : String(value ?? "");
      td.title = td.textContent;
      tr.append(td);
    });
    const actions = document.createElement("td");
    actions.className = "row-actions";
    if (state.view === "source") {
      const edit = document.createElement("button");
      edit.textContent = "Edit";
      edit.addEventListener("click", () => editRow(rows[index]));
      const remove = document.createElement("button");
      remove.textContent = "Delete";
      remove.className = "delete";
      remove.addEventListener("click", () => deleteRow(rows[index]));
      actions.append(edit, remove);
    } else {
      actions.textContent = row.is_deleted ? "Protected" : "Mirrored";
    }
    tr.append(actions);
    body.append(tr);
  });
  table.append(head, body);
}

function primaryKey(row) {
  return Object.fromEntries(state.table.primary_key_columns.map((name) => [name, row[name]]));
}

async function editRow(row) {
  const proposed = prompt("Edit values as JSON:", JSON.stringify(row, null, 2));
  if (!proposed) return;
  try {
    const values = JSON.parse(proposed);
    await mutateRow("PATCH", { primary_key: primaryKey(row), values });
  } catch (error) {
    flash(error.message, true);
  }
}

async function deleteRow(row) {
  if (!confirm(`Delete row ${JSON.stringify(primaryKey(row))} from source? The replica copy will be protected.`)) return;
  await mutateRow("DELETE", { primary_key: primaryKey(row) });
}

async function mutateRow(method, body) {
  const path = `/api/connections/${state.connection.id}/rows/${encodeURIComponent(state.table.schema_name)}/${encodeURIComponent(state.table.table_name)}`;
  try {
    await api(path, { method, body: JSON.stringify(body) });
    flash(method === "DELETE" ? "Deleted from source; CDC will preserve it in the replica audit." : "Source row changed; CDC is propagating it.");
    setTimeout(() => { loadRows(); loadHealth(); }, 1200);
  } catch (error) {
    flash(error.message, true);
  }
}

$("add-row").addEventListener("click", async () => {
  const proposed = prompt("New row values as JSON:", "{\n  \n}");
  if (!proposed) return;
  try {
    await mutateRow("POST", { values: JSON.parse(proposed) });
  } catch (error) {
    flash(error.message, true);
  }
});

document.querySelectorAll(".tab").forEach((tab) => tab.addEventListener("click", async () => {
  state.view = tab.dataset.view;
  document.querySelectorAll(".tab").forEach((item) => item.classList.toggle("active", item === tab));
  $("add-row").classList.toggle("hidden", state.view !== "source");
  await loadRows();
}));
$("refresh").addEventListener("click", () => Promise.all([loadTables(), loadHealth(), state.table && loadRows()]));
boot();
