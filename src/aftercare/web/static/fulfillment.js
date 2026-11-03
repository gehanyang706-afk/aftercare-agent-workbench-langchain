const $ = (selector) => document.querySelector(selector);

const state = {
  options: { demo_customer_login: false, warehouse_simulation: false },
  session: null,
  items: [],
  cursors: [null],
  page: 0,
  nextCursor: null,
  selectedId: null,
  detail: null,
  pending: null,
  busy: false,
};

const statusLabels = {
  EXCHANGE_REQUESTED: "待客服审核",
  AWAITING_RETURN: "待客户寄回",
  RETURN_IN_TRANSIT: "待仓库收货",
  RETURN_RECEIVED: "已收货 · 待出库",
  REPLACEMENT_PENDING: "出库结果待确认",
  REPLACEMENT_SHIPPED: "已记录换货出库",
  COMPLETED: "换货记录已完成",
  REJECTED: "申请已拒绝",
  WITHDRAWN: "申请已撤回",
};

const statusDescriptions = {
  EXCHANGE_REQUESTED: "申请已提交，等待客服审核。",
  AWAITING_RETURN: "审核已通过，请填写寄回商品的物流信息。",
  RETURN_IN_TRANSIT: "已记录寄回信息，等待仓库收货。",
  RETURN_RECEIVED: "仓库已记录收货，等待提交换货出库请求。",
  REPLACEMENT_PENDING: "出库请求已记录，尚未确认发出。",
  REPLACEMENT_SHIPPED: "已记录换货出库，请在收到换货商品后确认完成。",
  COMPLETED: "本次换货记录已完成，可在下方查看处理凭据。",
  REJECTED: "申请未通过审核，本次换货处理已结束。",
  WITHDRAWN: "申请已撤回，本次换货处理已结束。",
};

const roleLabels = { customer: "客户", support: "客服", warehouse: "仓库", system: "历史记录" };
const actionDefinitions = {
  approve: {
    title: "审核通过", description: "核对申请后，通知客户寄回商品。", path: "review",
    fields: [{ name: "note", label: "审核说明（选填）", type: "textarea", max: 500 }],
    extra: { decision: "approve" },
  },
  reject: {
    title: "拒绝申请", description: "记录审核意见，结束本次换货申请。", path: "review", danger: true,
    fields: [{ name: "note", label: "拒绝原因（选填）", type: "textarea", max: 500 }],
    extra: { decision: "reject" },
  },
  withdraw: {
    title: "撤回申请", description: "撤回后将停止本次换货处理。", path: "withdraw", danger: true,
  },
  submit_return: {
    title: "保存寄回信息", description: "请填写已寄回商品的物流信息。", path: "return-shipment",
    fields: [
      { name: "carrier", label: "物流公司", max: 100, required: true },
      { name: "tracking_number", label: "寄回运单号", max: 100, required: true },
    ],
  },
  receive: {
    title: "确认收货", description: "请核对退回商品后，记录收货。", path: "receive",
    fields: [{ name: "note", label: "收货说明（选填）", type: "textarea", max: 500 }],
  },
  request_shipment: {
    title: "提交出库请求", description: "先记录出库请求，进度将依据出库结果更新。", path: "replacement-shipments",
  },
  simulate_shipment: {
    title: "本地模拟出库", description: "生成本地模拟出库记录，不连接真实物流。", path: "simulate-shipment", simulated: true,
  },
  complete: {
    title: "确认收到换货", description: "请核对已收到的换货商品，再确认完成。", path: "complete",
  },
};

const errorMessages = {
  AUTH_FAILED: "账号或密码不正确，请重新输入。",
  ACCOUNT_DISABLED: "此员工账号已停用，请联系负责人。",
  SESSION_REQUIRED: "请先登录后查看申请。",
  AMBIGUOUS_IDENTITY: "当前登录状态需要重新确认，请重新登录。",
  INVALID_INPUT: "请检查填写的信息后重试。",
  STAFF_INPUT_INVALID: "请检查账号与密码格式后重试。",
  IDEMPOTENCY_KEY_REQUIRED: "此次操作缺少必要校验，请刷新页面后重试。",
  INVALID_CREDENTIALS: "账号或密码不正确，请重新输入。",
  LOGIN_FAILED: "账号或密码不正确，请重新输入。",
  AUTHENTICATION_FAILED: "账号或密码不正确，请重新输入。",
  STAFF_LOGIN_FAILED: "账号或密码不正确，请重新输入。",
  STAFF_SESSION_REQUIRED: "请先使用员工账号登录。",
  DEMO_SESSION_REQUIRED: "请先登录后查看申请。",
  SESSION_EXPIRED: "登录已失效，请重新登录。",
  DEMO_SESSION_EXPIRED: "登录已失效，请重新登录。",
  STAFF_SESSION_EXPIRED: "登录已失效，请重新登录。",
  DEMO_LOGIN_DISABLED: "本地演示客户入口未启用。",
  DEMO_CUSTOMER_LOGIN_DISABLED: "本地演示客户入口未启用。",
  WAREHOUSE_SIMULATION_DISABLED: "本地模拟出库未启用。",
  FORBIDDEN: "当前身份不能执行此操作。",
  CSRF_REQUIRED: "登录校验已变化，请刷新页面后重试。",
  ORIGIN_DENIED: "此请求未通过来源校验，请从本工作台操作。",
  NOT_FOUND: "未找到这条申请，或当前身份无权查看。",
  VERSION_CONFLICT: "申请已被更新，请查看最新记录后重新操作。",
  ILLEGAL_TRANSITION: "当前进度不允许执行此操作，请刷新记录。",
  IDEMPOTENCY_CONFLICT: "此次操作与先前请求不一致，请刷新并核对处理记录。",
  INVENTORY_INVARIANT: "库存或申请记录需要核对，本次操作未完成。",
  SHIPMENT_CONFLICT: "出库单号存在冲突，本次操作未完成。",
  SHIPMENT_NOT_REQUESTED: "尚未记录出库请求，请先刷新核对当前进度。",
  INVALID_COMMAND_DATA: "请检查填写的信息后重试。",
  VALIDATION_ERROR: "请检查填写的信息后重试。",
};

class RequestError extends Error {
  constructor(status, code, message) {
    super(message || "请求未完成");
    this.status = status;
    this.code = code;
    this.uncertain = status === 0 || status >= 500;
  }
}

function node(tag, text, className) {
  const element = document.createElement(tag);
  if (text !== undefined) element.textContent = text;
  if (className) element.className = className;
  return element;
}

function feedback(message, kind = "") {
  const element = $("#feedback");
  element.textContent = message;
  element.className = `feedback ${kind}`;
  element.hidden = !message;
  element.setAttribute("role", kind === "error" ? "alert" : "status");
}

function errorText(error) {
  if (errorMessages[error.code]) return errorMessages[error.code];
  if (error.status === 401) return "登录已失效，请重新登录。";
  if (error.status === 403) return "当前身份无法执行此操作，请重新登录或核对权限。";
  if (error.status === 422) return "请检查填写的信息后重试。";
  if (error.status === 0) return "连接中断，请稍后刷新查看最新记录。";
  if (error.status >= 500) return "暂时无法完成请求，请稍后查看最新记录。";
  if (error instanceof RequestError && !/^[A-Z_]+$/.test(error.message)) return error.message;
  return "请求未完成，请刷新记录后重试。";
}

function showError(error) {
  if (error.status === 401) loggedOut();
  feedback(errorText(error), "error");
}

async function api(path, { method = "GET", body, key } = {}) {
  const headers = {};
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (method !== "GET" && state.session) headers["X-CSRF-Token"] = state.session.csrf_token;
  if (key) headers["Idempotency-Key"] = key;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  let response;
  try {
    response = await fetch(path, {
      method, headers, credentials: "same-origin", cache: "no-store", signal: controller.signal,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    const text = await response.text();
    let data = null;
    if (text) {
      try { data = JSON.parse(text); }
      catch { throw new RequestError(response.ok ? 0 : response.status, "INVALID_RESPONSE"); }
    }
    if (!response.ok) {
      throw new RequestError(response.status, data?.error_code, data?.user_message || data?.detail);
    }
    return data;
  } catch (error) {
    if (error instanceof RequestError) throw error;
    throw new RequestError(0, "CONNECTION_LOST");
  } finally {
    clearTimeout(timeout);
  }
}

function updateDisabled() {
  for (const button of document.querySelectorAll("#customer-login-form button, #staff-login-form button, #refresh, #logout, #pending-refresh, #retry-operation, .application-choice")) {
    button.disabled = state.busy;
  }
  $("#previous-page").disabled = state.busy || state.page === 0;
  $("#next-page").disabled = state.busy || !state.nextCursor;
  for (const button of document.querySelectorAll("#action-controls button")) button.disabled = state.busy || Boolean(state.pending);
}

async function runBusy(task) {
  if (state.busy) return;
  state.busy = true;
  updateDisabled();
  try { await task(); }
  finally { state.busy = false; updateDisabled(); }
}

function identityKey(session) {
  return session ? `${session.role}:${session.actor_id}` : "";
}

function loggedOut() {
  state.session = null;
  state.pending = null;
  state.detail = null;
  state.selectedId = null;
  state.items = [];
  state.page = 0;
  state.cursors = [null];
  state.nextCursor = null;
  $("#workspace").hidden = true;
  $("#login-panel").hidden = false;
  $("#staff-password").value = "";
  renderPending();
}

function renderSession() {
  $("#login-panel").hidden = true;
  $("#workspace").hidden = false;
  $("#current-role").textContent = roleLabels[state.session.role] || "已登录";
  $("#current-user").textContent = state.session.display_name || state.session.actor_id;
  $("#create-application").hidden = state.session.role !== "customer";
  $("#list-empty-description").textContent = state.session.role === "customer"
    ? "请先在售后申请工作台提交并确认换货申请，然后回来刷新。"
    : "当前没有可查看的换货申请。客户确认提交后，申请会显示在这里。";
}

async function loadOptions() {
  state.options = await api("/api/auth/options");
  $("#customer-login-box").hidden = !state.options.demo_customer_login;
  $("#customer-login-disabled").hidden = Boolean(state.options.demo_customer_login);
  $("#login-grid").classList.toggle("single", !state.options.demo_customer_login);
  $("#environment-note").textContent = state.options.warehouse_simulation
    ? "本地模拟出库已启用；不连接真实物流。"
    : "本页展示已确认提交的换货申请。";
}

async function restoreSession() {
  const session = await api("/api/fulfillment/session");
  if (identityKey(state.session) !== identityKey(session)) {
    state.pending = null;
    state.selectedId = null;
    state.detail = null;
    state.page = 0;
    state.cursors = [null];
  }
  state.session = session;
  renderSession();
}

function statusClass(status) {
  if (status === "COMPLETED" || status === "REPLACEMENT_SHIPPED") return "status-badge done";
  if (status === "REJECTED" || status === "WITHDRAWN") return "status-badge closed";
  return "status-badge";
}

function renderList() {
  const list = $("#applications-list");
  list.replaceChildren();
  $("#applications-empty").hidden = state.items.length > 0;
  $("#page-count").textContent = `当前页 ${state.items.length} 条`;
  $("#page-number").textContent = `第 ${state.page + 1} 页`;
  for (const item of state.items) {
    const row = node("li");
    const button = node("button", undefined, `application-choice${state.selectedId === item.application_id ? " selected" : ""}`);
    button.type = "button";
    button.setAttribute("aria-pressed", String(state.selectedId === item.application_id));
    const top = node("span", undefined, "application-choice-top");
    top.append(node("strong", item.order_id || "换货申请"), node("span", statusLabels[item.status] || "处理中", statusClass(item.status)));
    button.append(top, node("p", `${item.quantity} 件 · ${item.target_variant || "换货商品"}`), node("small", item.application_id, "application-short"));
    button.addEventListener("click", () => runBusy(async () => {
      feedback("");
      await loadDetail(item.application_id);
    }).catch(showError));
    row.append(button);
    list.append(row);
  }
  updateDisabled();
}

function facts(element, entries) {
  element.replaceChildren();
  for (const [label, value] of entries) {
    element.append(node("dt", label), node("dd", value === undefined || value === null || value === "" ? "尚未记录" : String(value)));
  }
}

function displayTime(value) {
  if (!value) return "时间未记录";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "时间未记录";
  return new Intl.DateTimeFormat("zh-CN", { timeZone: "Asia/Shanghai", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }).format(date);
}

function renderHistory(history) {
  const list = $("#history-list");
  list.replaceChildren();
  $("#history-count").textContent = `${history.length} 条记录`;
  for (const record of history) {
    const item = node("li", undefined, "history-item");
    const top = node("div", undefined, "history-top");
    const title = record.command === "legacy_import" ? "历史履约记录导入"
      : record.command === "demo_settle" ? `演示处理 · ${statusLabels[record.to_state] || "状态已记录"}`
        : record.command === "application_created" ? "申请已确认提交"
          : statusLabels[record.to_state] || "处理记录已更新";
    const time = node("time", displayTime(record.created_at));
    if (record.created_at) time.dateTime = record.created_at;
    top.append(node("span", title, "history-title"), time);
    const actor = record.actor_source === "trusted_adapter" ? `本地模拟仓库${record.details?.triggered_by?.actor_id ? ` · 由仓库 ${record.details.triggered_by.actor_id} 发起` : ""}`
      : record.actor_source === "migration" ? "历史记录导入时间"
        : record.actor_source === "demo_helper" ? "演示辅助处理，未补充物流凭据"
          : `${roleLabels[record.actor_role] || "处理人"} · ${record.actor_id}`;
    item.append(top, node("p", actor));
    if (record.details?.data?.note) item.append(node("p", record.details.data.note));
    list.append(item);
  }
  if (!history.length) list.append(node("li", "暂无处理记录。", "muted"));
}

function renderActions() {
  const container = $("#action-controls");
  container.replaceChildren();
  const allowed = (state.detail?.allowed_actions || []).filter((name) => actionDefinitions[name]
    && (name !== "simulate_shipment" || state.options.warehouse_simulation));
  $("#no-actions").hidden = allowed.length > 0;
  $("#action-hint").textContent = state.pending ? "先核实上一次操作结果" : "按当前身份与申请进度显示";
  for (const name of allowed) {
    const definition = actionDefinitions[name];
    const form = node("form", undefined, `action-card${definition.simulated ? " simulated" : ""}`);
    form.append(node("h4", definition.title), node("p", definition.description));
    for (const field of definition.fields || []) {
      const id = `action-${name}-${field.name}`;
      const label = node("label", field.label);
      label.htmlFor = id;
      const input = node(field.type === "textarea" ? "textarea" : "input");
      input.id = id;
      input.name = field.name;
      input.maxLength = field.max;
      input.required = Boolean(field.required);
      if (field.type !== "textarea") input.type = "text";
      form.append(label, input);
    }
    const button = node("button", definition.title, definition.danger ? "button-danger" : "");
    button.type = "submit";
    form.append(button);
    form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (state.busy || state.pending) return;
      const body = { expected_version: state.detail.version, ...(definition.extra || {}) };
      for (const field of definition.fields || []) body[field.name] = form.elements.namedItem(field.name).value.trim();
      const operation = {
        applicationId: state.selectedId, action: name, body,
        path: `/api/applications/${encodeURIComponent(state.selectedId)}/${definition.path}`,
        key: `ui_${crypto.randomUUID()}`, identity: identityKey(state.session), uncertain: false,
      };
      state.pending = operation;
      runBusy(() => sendOperation(operation)).catch(showError);
    });
    container.append(form);
  }
  updateDisabled();
}

function renderDetail() {
  const detail = state.detail;
  $("#detail-empty").hidden = Boolean(detail);
  $("#application-detail").hidden = !detail;
  if (!detail) return;
  $("#application-number").textContent = detail.application_id;
  $("#application-status").textContent = statusLabels[detail.status] || "处理中";
  $("#application-status").className = statusClass(detail.status);
  $("#status-description").textContent = detail.fulfillment?.shipment_status === "UNKNOWN"
    ? "出库结果尚待核实，请等待确认后再处理后续环节。"
    : statusDescriptions[detail.status] || "请查看下方已记录的处理进度。";
  facts($("#application-facts"), [["原订单", detail.order_id], ["申请数量", `${detail.quantity} 件`], ["换货商品", detail.target_label || detail.target_variant]]);
  const fulfillment = detail.fulfillment || {};
  $("#historical-note").hidden = fulfillment.origin !== "legacy" && !(detail.history || []).some((row) => row.actor_source === "demo_helper");
  $("#simulation-label").hidden = !String(fulfillment.external_shipment_id || "").startsWith("SIM-");
  facts($("#fulfillment-facts"), [
    ["寄回物流", fulfillment.return_carrier], ["寄回运单号", fulfillment.return_tracking],
    ["仓库收货时间", fulfillment.received_at ? displayTime(fulfillment.received_at) : null],
    ["收货说明", fulfillment.received_note], ["换货出库单号", fulfillment.external_shipment_id],
  ]);
  const receipt = detail.receipt || {};
  facts($("#submission-facts"), [["原订单", receipt.order_id || detail.order_id], ["提交编号", receipt.operation_id], ["提交结果", receipt.result === "EXCHANGE_REQUESTED" ? "换货申请已提交" : "以原提交记录为准"]]);
  $("#submission-notice").textContent = receipt.notice || "原提交凭据记录申请提交事实，后续履约进度请查看当前记录。";
  renderActions();
  renderHistory(detail.history || []);
}

async function loadDetail(applicationId) {
  state.selectedId = applicationId;
  state.detail = null;
  renderList();
  renderDetail();
  if (!applicationId) return;
  state.detail = await api(`/api/applications/${encodeURIComponent(applicationId)}`);
  renderDetail();
}

async function loadPage(page = state.page, after = state.cursors[page], preferredId = state.selectedId) {
  const query = new URLSearchParams({ limit: "20" });
  if (after) query.set("after", after);
  const result = await api(`/api/applications?${query}`);
  state.items = result.items || [];
  state.page = page;
  state.cursors[page] = after || null;
  state.nextCursor = result.next_cursor || null;
  const selected = preferredId || state.items[0]?.application_id || null;
  await loadDetail(selected);
}

function renderPending() {
  $("#pending-operation").hidden = !state.pending?.uncertain;
  if (state.pending?.uncertain) {
    $("#pending-description").textContent = `“${actionDefinitions[state.pending.action].title}”的结果尚未确认。刷新可查看最新记录；重试将沿用原操作，不会撤销已完成的处理。`;
  }
  updateDisabled();
}

async function refreshRecords() {
  await restoreSession();
  await loadPage(state.page, state.cursors[state.page], state.pending?.applicationId || state.selectedId);
}

async function sendOperation(operation) {
  if (identityKey(state.session) !== operation.identity) {
    state.pending = null;
    renderPending();
    throw new RequestError(401, "SESSION_EXPIRED");
  }
  feedback("");
  try {
    const receipt = await api(operation.path, { method: "POST", body: operation.body, key: operation.key });
    if (!receipt || receipt.application_id !== operation.applicationId || !Number.isInteger(receipt.version)) {
      throw new RequestError(0, "INVALID_RESPONSE");
    }
  } catch (error) {
    if (error.uncertain) {
      operation.uncertain = true;
      state.pending = operation;
      feedback("操作结果尚未确认，请先查看最新记录，或重试原操作。", "error");
      renderPending();
      return;
    }
    state.pending = null;
    renderPending();
    if (error.status !== 401) {
      try { await refreshRecords(); } catch { /* The command error remains the primary feedback. */ }
    }
    throw error;
  }
  state.pending = null;
  renderPending();
  try {
    await refreshRecords();
    feedback("操作已记录，当前进度已刷新。", "success");
  } catch (error) {
    if (error.status === 401) loggedOut();
    feedback("操作已记录，但暂时无法读取最新进度。请刷新记录查看。", "error");
  }
}

$("#customer-login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  runBusy(async () => {
    feedback("");
    await api("/api/demo-sessions", { method: "POST", body: { customer_id: $("#customer-id").value } });
    state.pending = null;
    await restoreSession();
    await loadPage(0, null, null);
  }).catch(showError);
});

$("#staff-login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  runBusy(async () => {
    feedback("");
    const password = $("#staff-password").value;
    try {
      await api("/api/staff/sessions", { method: "POST", body: { username: $("#staff-username").value.trim(), password } });
    } finally {
      $("#staff-password").value = "";
    }
    state.pending = null;
    await restoreSession();
    await loadPage(0, null, null);
  }).catch(showError);
});

$("#logout").addEventListener("click", () => runBusy(async () => {
  const path = state.session.role === "customer" ? "/api/session" : "/api/staff/session";
  await api(path, { method: "DELETE" });
  loggedOut();
  feedback("已退出登录。");
}).catch(showError));

for (const selector of ["#refresh", "#pending-refresh"]) {
  $(selector).addEventListener("click", () => runBusy(async () => {
    feedback("");
    await loadOptions();
    await refreshRecords();
  }).catch(showError));
}

$("#retry-operation").addEventListener("click", () => {
  if (!state.pending || state.busy) return;
  runBusy(() => sendOperation(state.pending)).catch(showError);
});

$("#previous-page").addEventListener("click", () => runBusy(async () => {
  feedback("");
  await loadPage(state.page - 1, state.cursors[state.page - 1], null);
}).catch(showError));

$("#next-page").addEventListener("click", () => runBusy(async () => {
  feedback("");
  await loadPage(state.page + 1, state.nextCursor, null);
}).catch(showError));

runBusy(async () => {
  await loadOptions();
  try { await restoreSession(); }
  catch (error) {
    if (error.status === 401) { loggedOut(); return; }
    throw error;
  }
  await loadPage(0, null, null);
}).catch(showError);
