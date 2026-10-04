// Supervisor review of labour hours by activity code. Each entry waiting for review shows its lines (activity,
// hours, note); the supervisor fixes codes or splits hours, then saves or approves. Lines must add up to the
// entry's hours, so the billing report always matches the hours paid. Only approved entries go on the report.
import { LABOUR_ACTIVITIES, LABOUR_CATEGORIES } from "./labour-activity-codes.js";

const ACTIVITY_BY_CODE = new Map(LABOUR_ACTIVITIES.map((a) => [a.code, a]));

function esc(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function hoursText(minutes) {
  return String(Math.round((Number(minutes) || 0) / 60 * 100) / 100);
}

function entryMinutes(entry) {
  const m = Number(entry && entry.minutesWorked);
  if (Number.isFinite(m) && m > 0) return Math.round(m);
  const h = Number(entry && entry.hours);
  return Number.isFinite(h) && h > 0 ? Math.round(h * 60) : 0;
}

function activityOptions(selected) {
  const groups = LABOUR_CATEGORIES.map((c) => {
    const opts = LABOUR_ACTIVITIES.filter((a) => a.category === c.id)
      .map((a) => `<option value="${a.code}"${a.code === selected ? " selected" : ""}>${esc(a.label)} [${esc(a.keyword)}]</option>`).join("");
    return `<optgroup label="${esc(`${c.label} (${c.note})`)}">${opts}</optgroup>`;
  }).join("");
  return `<option value=""${selected ? "" : " selected"}>Needs a code</option>${groups}`;
}

function lineRow(line) {
  return `<div class="labour-line">
      <select class="labour-line-code" aria-label="Activity">${activityOptions(line.code || "")}</select>
      <input class="labour-line-hours" type="number" min="0.25" max="24" step="0.25" value="${esc(line.minutes ? hoursText(line.minutes) : "")}" aria-label="Hours">
      <input class="labour-line-text" type="text" maxlength="300" value="${esc(line.text || "")}" placeholder="Note" aria-label="Note">
      <button type="button" class="btn-secondary labour-line-remove" aria-label="Remove line">×</button>
    </div>`;
}

/** Entries waiting for review: those saved since activity codes began, not yet approved. Oldest day first. */
export function labourEntriesToReview(entries) {
  return (entries || [])
    .filter((e) => e && e.review && e.review.status !== "approved")
    .sort((a, b) => String(a.reportDateKey || "").localeCompare(String(b.reportDateKey || "")) || String(a.labourerName || "").localeCompare(String(b.labourerName || "")));
}

function entryCard(entry, labourerLabel) {
  const lines = Array.isArray(entry.lines) && entry.lines.length ? entry.lines : [{ code: "", minutes: entryMinutes(entry), text: entry.workOn || "" }];
  return `<div class="row-item labour-review-entry" data-review-entry-id="${esc(entry.id)}" data-review-minutes="${entryMinutes(entry)}">
      <div><span class="pill pill-issue">${esc(hoursText(entryMinutes(entry)))}h</span><span class="pill pill-ai">${esc(entry.reportDateKey || "-")}</span> <strong>${esc(labourerLabel(entry))}</strong> <span class="muted small">· ${esc(entry.source || "")}</span></div>
      <div class="muted small">They wrote: ${esc(String(entry.workOn || "").slice(0, 400))}</div>
      <div class="labour-lines">${lines.map(lineRow).join("")}</div>
      <div class="labour-review-actions">
        <button type="button" class="btn-secondary labour-line-add">Add line</button>
        <span class="labour-review-sum small"></span>
        <button type="button" class="btn-secondary labour-review-save">Save</button>
        <button type="button" class="btn-primary labour-review-approve">Approve</button>
      </div>
      <div class="labour-review-result small" aria-live="polite"></div>
    </div>`;
}

function readLines(card) {
  return [...card.querySelectorAll(".labour-line")].map((row) => ({
    code: row.querySelector(".labour-line-code").value,
    hours: Number(row.querySelector(".labour-line-hours").value),
    text: row.querySelector(".labour-line-text").value.trim(),
  }));
}

function updateSum(card) {
  const total = Number(card.dataset.reviewMinutes) || 0;
  const lines = readLines(card);
  const sum = lines.reduce((s, l) => s + (Number.isFinite(l.hours) ? Math.round(l.hours * 60) : 0), 0);
  const uncoded = lines.filter((l) => !ACTIVITY_BY_CODE.has(l.code)).length;
  const el = card.querySelector(".labour-review-sum");
  const ok = sum === total;
  el.textContent = `Lines: ${hoursText(sum)}h of ${hoursText(total)}h${uncoded ? ` · ${uncoded} need a code` : ""}`;
  el.className = `labour-review-sum small ${ok && !uncoded ? "ok" : "err"}`;
  card.querySelector(".labour-review-approve").disabled = !ok || uncoded > 0;
}

/**
 * Renders the review list. Cards the supervisor is editing are kept as they are, so a live update to other
 * entries doesn't wipe their changes.
 */
export function renderLabourReview(container, entries, { labourerLabel = (e) => e.labourerName || e.labourerPhone || "Unknown" } = {}) {
  if (!container) return;
  const list = labourEntriesToReview(entries);
  const editing = new Map([...container.querySelectorAll(".labour-review-entry[data-dirty='1']")].map((el) => [el.dataset.reviewEntryId, el]));
  if (!list.length) {
    container.innerHTML = '<div class="row-item muted">Nothing to review. New hours appear here as they come in.</div>';
    return;
  }
  const holder = document.createElement("div");
  holder.innerHTML = list.map((e) => (editing.has(e.id) ? `<div data-keep="${esc(e.id)}"></div>` : entryCard(e, labourerLabel))).join("");
  holder.querySelectorAll("[data-keep]").forEach((slot) => slot.replaceWith(editing.get(slot.dataset.keep)));
  container.replaceChildren(...holder.children);
  container.querySelectorAll(".labour-review-entry").forEach(updateSum);
}

/** Wires the buttons once. `call(name, payload)` calls a Cloud Function. */
export function bindLabourReview(container, call) {
  if (!container || container.dataset.bound) return;
  container.dataset.bound = "1";
  const markDirty = (card) => { card.dataset.dirty = "1"; };
  container.addEventListener("input", (event) => {
    const card = event.target.closest(".labour-review-entry");
    if (!card) return;
    markDirty(card);
    updateSum(card);
  });
  container.addEventListener("click", async (event) => {
    const card = event.target.closest(".labour-review-entry");
    if (!card) return;
    if (event.target.closest(".labour-line-add")) {
      card.querySelector(".labour-lines").insertAdjacentHTML("beforeend", lineRow({ code: "", minutes: 0, text: "" }));
      markDirty(card);
      updateSum(card);
      return;
    }
    if (event.target.closest(".labour-line-remove")) {
      const rows = card.querySelectorAll(".labour-line");
      if (rows.length > 1) event.target.closest(".labour-line").remove();
      markDirty(card);
      updateSum(card);
      return;
    }
    const approve = !!event.target.closest(".labour-review-approve");
    if (!approve && !event.target.closest(".labour-review-save")) return;
    const result = card.querySelector(".labour-review-result");
    const buttons = card.querySelectorAll("button");
    buttons.forEach((b) => { b.disabled = true; });
    result.textContent = approve ? "Approving..." : "Saving...";
    result.className = "labour-review-result small muted";
    try {
      await call("reviewLabourEntryCallable", { entryId: card.dataset.reviewEntryId, lines: readLines(card), approve });
      delete card.dataset.dirty;
      result.textContent = approve ? "Approved." : "Saved. Still waiting for approval.";
      result.className = "labour-review-result small ok";
    } catch (err) {
      result.textContent = `Not saved: ${err && err.message ? err.message : err}`;
      result.className = "labour-review-result small err";
    } finally {
      buttons.forEach((b) => { b.disabled = false; });
      updateSum(card);
    }
  });
}

/** One-line summary of an entry's coded lines for the entries list, e.g. "Winter Heat 6h · Other Work 3h". */
export function labourLinesSummary(entry) {
  if (!entry || !Array.isArray(entry.lines) || !entry.lines.length) return "";
  const byCat = new Map();
  let uncoded = 0;
  for (const line of entry.lines) {
    const a = ACTIVITY_BY_CODE.get(line.code);
    if (a) byCat.set(a.category, (byCat.get(a.category) || 0) + (Number(line.minutes) || 0));
    else uncoded += Number(line.minutes) || 0;
  }
  const parts = LABOUR_CATEGORIES.filter((c) => byCat.get(c.id)).map((c) => `${c.label} ${hoursText(byCat.get(c.id))}h`);
  if (uncoded) parts.push(`uncoded ${hoursText(uncoded)}h`);
  const status = entry.review && entry.review.status === "approved" ? "approved" : "waiting for review";
  return `${parts.join(" · ")} · ${status}`;
}
