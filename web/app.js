const currency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});
const number = new Intl.NumberFormat("en-US");

const elements = {
  connection: document.querySelector("#connection-label"),
  connectionDot: document.querySelector(".status-dot"),
  notice: document.querySelector("#notice"),
  runButton: document.querySelector("#run-button"),
  refreshButton: document.querySelector("#refresh-button"),
  source: document.querySelector("#source-select"),
};

function escapeHTML(value) {
  return String(value).replace(/[&<>"']/g, (character) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  })[character]);
}

function showNotice(message, isError = false) {
  elements.notice.textContent = message;
  elements.notice.classList.toggle("error", isError);
  elements.notice.hidden = false;
}

function hideNotice() {
  elements.notice.hidden = true;
  elements.notice.classList.remove("error");
}

function setTable(id, rows, columns, emptyMessage) {
  const body = document.querySelector(id);
  if (!rows.length) {
    body.innerHTML = `<tr><td class="empty-cell" colspan="${columns.length}">${emptyMessage}</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((row) => `<tr>${columns.map((column) => `<td>${column(row)}</td>`).join("")}</tr>`).join("");
}

function renderSales(rows) {
  const chart = document.querySelector("#sales-chart");
  if (!rows.length) {
    chart.innerHTML = '<p class="empty-message">Run the pipeline to create your first sales report.</p>';
    return;
  }
  const max = Math.max(...rows.map((row) => Number(row.net_revenue)), 1);
  chart.innerHTML = rows.map((row) => {
    const height = Math.max(Number(row.net_revenue) / max * 100, 2);
    const date = escapeHTML(row.order_date);
    return `<div class="bar-column" title="${date}: ${currency.format(row.net_revenue)}"><span class="bar" style="height:${height}%"></span><span class="bar-label">${date.slice(5)}</span></div>`;
  }).join("");
}

function renderProducts(rows) {
  const chart = document.querySelector("#products-chart");
  const topRows = [...rows].sort((a, b) => Number(b.net_revenue) - Number(a.net_revenue)).slice(0, 5);
  if (!topRows.length) {
    chart.innerHTML = '<p class="empty-message">Product data will appear after a pipeline run.</p>';
    return;
  }
  const max = Math.max(...topRows.map((row) => Number(row.net_revenue)), 1);
  chart.innerHTML = topRows.map((row) => `
    <div class="rank-row">
      <span class="rank-name" title="${escapeHTML(row.product_name)}">${escapeHTML(row.product_name)}</span>
      <span class="rank-track"><span class="rank-fill" style="display:block;width:${Number(row.net_revenue) / max * 100}%"></span></span>
      <span class="rank-value">${currency.format(row.net_revenue)}</span>
    </div>`).join("");
}

function renderDashboard(data) {
  const { kpis, tables, quality } = data;
  const mysqlOption = elements.source.querySelector('option[value="mysql"]');
  if (data.features?.mysql === false) {
    mysqlOption.disabled = true;
    mysqlOption.textContent = "MySQL (disabled on public demo)";
    if (elements.source.value === "mysql") elements.source.value = "csv";
  }
  document.querySelector("#kpi-revenue").textContent = currency.format(kpis.revenue);
  document.querySelector("#kpi-orders").textContent = number.format(kpis.orders);
  document.querySelector("#kpi-aov").textContent = currency.format(kpis.average_order_value);
  document.querySelector("#kpi-repeat").textContent = `${kpis.repeat_customer_rate}%`;
  document.querySelector("#repeat-badge").textContent = `${kpis.repeat_customer_rate}% repeat`;

  renderSales(tables.daily_sales);
  renderProducts(tables.product_performance);

  const sortedCustomers = [...tables.customer_repeat_purchase].sort(
    (a, b) => Number(b.order_count) - Number(a.order_count) || a.customer_id.localeCompare(b.customer_id),
  );
  setTable("#customer-table", sortedCustomers.slice(0, 6), [
    (row) => `<span class="customer-id">${escapeHTML(row.customer_id)}</span>`,
    (row) => number.format(row.order_count),
    (row) => currency.format(row.lifetime_revenue),
    (row) => `<span class="segment ${row.is_repeat_customer === "true" ? "" : "new"}">${row.is_repeat_customer === "true" ? "Returning" : "New"}</span>`,
  ], "No customer data available.");

  const deliveries = tables.delivery_performance;
  const delivered = deliveries.reduce((sum, row) => sum + Number(row.delivered_order_items), 0);
  const onTime = deliveries.reduce((sum, row) => sum + Number(row.on_time_order_items), 0);
  const late = deliveries.reduce((sum, row) => sum + Number(row.late_order_items), 0);
  document.querySelector("#on-time-rate").textContent = `${kpis.on_time_rate}%`;
  document.querySelector("#delivery-progress").style.width = `${Math.min(kpis.on_time_rate, 100)}%`;
  document.querySelector("#on-time-count").textContent = number.format(onTime);
  document.querySelector("#late-count").textContent = number.format(late);
  setTable("#delivery-table", [...deliveries].reverse().slice(0, 5), [
    (row) => escapeHTML(row.order_date),
    (row) => `${Number(row.on_time_rate_pct).toFixed(1)}%`,
    (row) => `${Number(row.average_delay_days).toFixed(1)} days`,
  ], "No delivery data available.");

  const products = [...tables.product_performance].sort((a, b) => Number(b.net_revenue) - Number(a.net_revenue));
  document.querySelector("#product-count").textContent = `${products.length} products`;
  setTable("#product-table-body", products, [
    (row) => `<span class="customer-id">${escapeHTML(row.product_name)}</span><br><span class="table-subtext">${escapeHTML(row.product_id)}</span>`,
    (row) => escapeHTML(row.category || "—"),
    (row) => currency.format(row.net_revenue),
    (row) => number.format(row.units_sold),
    (row) => number.format(row.order_count),
  ], "No product data available.");

  const updated = quality?.extracted_at
    ? new Date(quality.extracted_at).toLocaleString()
    : "Waiting for analytics data";
  document.querySelector("#last-updated").textContent = `Last updated ${updated}`;
  if (quality) {
    document.querySelector("#connection-label").textContent = "Local API connected";
    elements.connectionDot.classList.remove("offline");
  }
}

async function loadDashboard() {
  elements.refreshButton.disabled = true;
  try {
    const response = await fetch("/api/dashboard", { cache: "no-store" });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not load dashboard");
    renderDashboard(data);
    elements.connection.textContent = "Local API connected";
    elements.connectionDot.classList.remove("offline");
    hideNotice();
  } catch (error) {
    elements.connection.textContent = "API unavailable";
    elements.connectionDot.classList.add("offline");
    showNotice(error.message, true);
  } finally {
    elements.refreshButton.disabled = false;
  }
}

async function runPipeline() {
  elements.runButton.disabled = true;
  elements.runButton.innerHTML = '<span aria-hidden="true">⟳</span> Running pipeline…';
  showNotice(elements.source.value === "mysql"
    ? "Connecting to MySQL and processing records…"
    : "Processing the sample data through Bronze, Silver, and Gold…");
  try {
    const response = await fetch("/api/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source: elements.source.value }),
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Pipeline run failed");
    renderDashboard(data.dashboard);
    const report = data.report;
    showNotice(`Pipeline complete — ${number.format(report.accepted_rows)} accepted, ${number.format(report.rejected_rows)} rejected, ${number.format(report.duplicate_rows)} duplicates.`);
  } catch (error) {
    showNotice(error.message, true);
  } finally {
    elements.runButton.disabled = false;
    elements.runButton.innerHTML = '<span aria-hidden="true">▶</span> Run pipeline';
  }
}

elements.refreshButton.addEventListener("click", loadDashboard);
elements.runButton.addEventListener("click", runPipeline);
loadDashboard();
