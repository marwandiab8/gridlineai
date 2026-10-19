// Docksteader morning briefing (9:30 AM) and evening wrap-up (6:30 PM), shown under Reports -> Briefing & wrap-up.
//
// The Procore Inbox Apps Script, running as Marwan's Google account, posts what only that account can read - the
// day's Docksteader email, the Docksteader Todoist project, the files the Procore Inbox saved and new Drive files -
// to docksteaderBriefing with a shared token (DOCKSTEADER_BRIEFING_TOKEN). That queues a run
// (docksteaderBriefingQueue); writeDocksteaderBriefing adds Site Logistics (bookings, tasks, the latest 3-week
// look-ahead, the daily note, labour hours - read through the runtime service account's datastore.viewer role on the
// site-logistics project), has OpenAI write the report and keeps it in docksteaderBriefings/{day}-{kind}, readable
// only by memberEmail (firestore.rules). The script polls docksteaderBriefing?run= until it's ready and emails it.
const crypto = require("crypto");
const { sanitizeChatCompletionParams } = require("./aiConfig");
const { completionText } = require("./openaiHelpers");

const COL_BRIEFINGS = "docksteaderBriefings";
const COL_QUEUE = "docksteaderBriefingQueue";
const TIME_ZONE = "America/Toronto";
// Site Logistics' Docksteader site, and the one member the reports are for.
const SITE_ID = "viWiEkc1KoPBnRds0zzh";
const MEMBER_EMAIL = "marwandiab8@gmail.com";
const MAX_MAIL_BODY = 700;
const MAX_INPUT_CHARS = 70000;

const KINDS = {
  briefing: {
    label: "Morning briefing",
    sections: ["Today on site", "Needs your reply", "Watch today", "Slipping or overdue"],
    focus:
      "the day ahead: who is on site today and doing what (grouped by sub), what is due or happening today (meetings, inspections, pours, deliveries, deadlines), what came in since last evening that needs Marwan's answer, and what is slipping.",
  },
  wrapup: {
    label: "Evening wrap-up",
    sections: ["Done today", "Slipped or overdue", "Needs your reply", "Tomorrow: subs on site"],
    focus:
      "the day that ended: what got done, what slipped or is overdue, what still needs Marwan's reply, and the next working day's work grouped by sub.",
  },
};

/** "2026-10-19" for a moment, in Toronto time. */
function dayKeyIn(date, timeZone = TIME_ZONE) {
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type).value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function addDays(dayKey, n) {
  const d = new Date(`${dayKey}T12:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}

/** "Mon Oct 19" */
function dayLabel(dayKey) {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: "UTC", weekday: "short", month: "short", day: "numeric" }).formatToParts(new Date(`${dayKey}T12:00:00Z`));
  const get = (type) => parts.find((p) => p.type === type).value;
  return `${get("weekday")} ${get("month")} ${get("day")}`;
}

function checkBearer(header, expected) {
  const m = /^Bearer\s+(.+)$/i.exec(String(header || "").trim());
  if (!m || !expected) return false;
  const a = Buffer.from(m[1].trim());
  const b = Buffer.from(String(expected));
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

function toDate(value) {
  if (!value) return null;
  if (typeof value.toDate === "function") return value.toDate();
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

const clip = (value, n) => String(value == null ? "" : value).replace(/\s+/g, " ").trim().slice(0, n);

/** The Site Logistics facts a report needs, from the site's raw documents: the report day and the 3 days after. */
function selectSiteData({ dayKey, siteName, areas, bookings, tasks, dayNote, labourHours, lookahead }) {
  const last = addDays(dayKey, 3);
  const area = (id) => (areas && areas[id]) || "";
  const overlaps = (start, end) => Boolean(start) && start <= last && (end || start) >= dayKey;
  const isDone = (t) => t.status === "done";
  const doneDay = (t) => {
    const d = toDate(t.doneAt);
    return d ? dayKeyIn(d) : "";
  };
  const task = (t) => ({ text: clip(t.text, 160), crew: [t.trade, t.company].filter(Boolean).join(" / "), area: area(t.areaId), day: t.day || "" });
  return {
    site: siteName || "Docksteader",
    bookings: (bookings || [])
      .filter((b) => overlaps(b.start, b.end))
      .map((b) => ({ company: b.company || "", trade: b.trade || "", activity: clip(b.activity || b.notes, 120), area: area(b.areaId), start: b.start, end: b.end || b.start }))
      .slice(0, 80),
    tasksDoneToday: (tasks || []).filter((t) => isDone(t) && doneDay(t) === dayKey).map((t) => ({ ...task(t), doneBy: t.doneBy || "" })),
    tasksOpen: (tasks || []).filter((t) => !isDone(t)).map(task).slice(0, 40),
    dayNote: clip(dayNote, 3000),
    labourHours: (labourHours || []).map((h) => ({
      by: h.byName || h.owner || "",
      status: h.status || "",
      hours: (h.lines || []).reduce((sum, l) => sum + (Number(l && l.hours) || 0), 0),
    })),
    lookahead: lookahead
      ? {
          file: lookahead.fileName || "",
          from: lookahead.from || "",
          to: lookahead.to || "",
          activities: (lookahead.activities || [])
            .filter((a) => overlaps(a.start, a.end) || /delay|hold|wait/i.test(a.status || ""))
            .map((a) => ({ zone: a.zone || "", name: clip(a.name, 100), company: a.company || "", status: clip(a.status, 80), start: a.start || "", end: a.end || "" }))
            .slice(0, 90),
          undated: (lookahead.undated || []).map((a) => ({ zone: a.zone || "", name: clip(a.name, 100), company: a.company || "", status: clip(a.status, 80) })).slice(0, 20),
        }
      : null,
  };
}

/** Reads the Docksteader site's documents from Site Logistics. */
async function loadSiteLogistics(slDb, dayKey) {
  const site = slDb.collection("sites").doc(SITE_ID);
  const [siteSnap, areaSnap, bookingSnap, taskSnap, noteSnap, hoursSnap, lookaheadSnap] = await Promise.all([
    site.get(),
    site.collection("items").where("type", "==", "workArea").get(),
    site.collection("bookings").get(),
    site.collection("tasks").get(),
    site.collection("dayNotes").doc(dayKey).get(),
    site.collection("labourHours").where("day", "==", dayKey).get(),
    site.collection("lookaheads").get(),
  ]);
  const areas = {};
  areaSnap.forEach((d) => { areas[d.id] = String(d.get("label") || "").trim(); });
  const all = (snap) => snap.docs.map((d) => ({ id: d.id, ...d.data() }));
  const latest = lookaheadSnap.docs.slice().sort((a, b) => b.createTime.toMillis() - a.createTime.toMillis())[0];
  return selectSiteData({
    dayKey,
    siteName: siteSnap.exists ? siteSnap.get("name") : "",
    areas,
    bookings: all(bookingSnap),
    tasks: all(taskSnap),
    dayNote: noteSnap.exists ? noteSnap.get("notes") : "",
    labourHours: all(hoursSnap),
    lookahead: latest ? latest.data() : null,
  });
}

/** Todoist as the report needs it, with a batch of tasks closed in the same minute marked as housekeeping. */
function shapeTodoist(todoist, dayKey) {
  if (!todoist || typeof todoist !== "object") return { note: "not included" };
  if (todoist.error || todoist.skipped) return { note: todoist.error || todoist.skipped };
  const last = addDays(dayKey, 3);
  const due = (t) => String(t.due || "").slice(0, 10);
  const open = (todoist.open || []).filter((t) => due(t) && due(t) <= last);
  const done = todoist.completedToday || [];
  const byMinute = {};
  done.forEach((t) => { const k = String(t.completedAt || "").slice(0, 16); byMinute[k] = (byMinute[k] || 0) + 1; });
  const batch = Object.entries(byMinute).filter(([, n]) => n >= 5).map(([at, count]) => ({ at, count }));
  const batchMinutes = new Set(batch.map((b) => b.at));
  const slim = (t) => ({ task: clip(t.content, 140), due: due(t), note: clip(t.description, 160) });
  return {
    openDueBy: last,
    overdue: open.filter((t) => due(t) < dayKey).map(slim).slice(0, 40),
    dueToday: open.filter((t) => due(t) === dayKey).map(slim),
    dueNextDays: open.filter((t) => due(t) > dayKey).map(slim).slice(0, 30),
    openWithoutDate: (todoist.open || []).filter((t) => !due(t)).length,
    completedToday: done.filter((t) => !batchMinutes.has(String(t.completedAt || "").slice(0, 16))).map(slim),
    batchClosed: batch,
  };
}

/** The INPUT block for the model, as compact JSON, cut to MAX_INPUT_CHARS by shortening email bodies. */
function buildBriefingInput({ kind, dayKey, nowLabel, payload, site }) {
  const mail = (payload.mail || []).map((thread) => ({
    subject: clip(thread.subject, 200),
    link: thread.link || "",
    unread: Boolean(thread.unread),
    messages: (thread.messages || []).map((m) => ({
      at: m.date || "",
      from: clip(m.from, 120),
      to: clip(m.to, 200),
      cc: clip(m.cc, 200),
      draft: Boolean(m.draft),
      attachments: (m.attachments || []).slice(0, 5),
      body: clip(m.body, MAX_MAIL_BODY),
    })),
  }));
  const input = {
    report: KINDS[kind].label,
    reportDay: dayKey,
    nextDay: addDays(dayKey, 1),
    writtenAt: nowLabel,
    emailSince: payload.windowStart || "",
    mail,
    todoist: shapeTodoist(payload.todoist, dayKey),
    procoreInboxSaved: (payload.procoreInbox || []).slice(0, 40),
    newDriveFiles: (payload.driveFiles || []).slice(0, 40),
    siteLogistics: site,
  };
  let text = JSON.stringify(input);
  for (let body = MAX_MAIL_BODY; text.length > MAX_INPUT_CHARS && body > 100; body -= 150) {
    input.mail.forEach((t) => t.messages.forEach((m) => { m.body = m.body.slice(0, body); }));
    text = JSON.stringify(input);
  }
  return text.slice(0, MAX_INPUT_CHARS);
}

function promptFor(kind, dayKey) {
  const k = KINDS[kind];
  return `Write Marwan Diab's ${k.label.toLowerCase()} for ${dayLabel(dayKey)}. Marwan is the Senior Project Superintendent for Matheson Constructors on the Docksteader PRPS Reporting Station (project 2025-048, 6 Docksteader Rd, Brampton). Cover ${k.focus}

Return JSON only:
{"headline": "one sentence: the single most important thing", "sections": [{"title": "...", "items": [{"lead": "short subject, e.g. a sub, item or person", "text": "one or two plain sentences", "link": "a URL copied from INPUT, or empty"}]}]}
Use exactly these sections, in this order: ${k.sections.map((s) => `"${s}"`).join(", ")}. Up to 12 items each; an empty items list is fine.

Rules:
- Use only facts in INPUT. Never invent dates, names, quantities, statuses or links; a link must be copied exactly from INPUT.
- "Done" means confirmed: an email, Procore notice or saved file, the daily note, labour hours, a Site Logistics task marked done, or a Todoist completion. Work that was only scheduled is not done; call it scheduled.
- todoist.batchClosed lists tasks closed together in one minute: that is clearing old items, not work done. Say so in one item if it matters.
- Compare the look-ahead and bookings with submittals, RFIs, deliveries and email, and flag conflicts (work scheduled before material arrives or before an approval or RFI answer is due back).
- Needs your reply: messages to or copying Marwan (mdiab@mathesonconstructors.com, marwandiab8@gmail.com) that ask him for something - an answer, a confirmation, an approval, a form, an invoice or pay application to review - and drafts (draft: true) that still need finishing. Leave out newsletters, marketing, automated notices that ask nothing, security alerts and the hourly digest emails.
- A message from "Marwan Diab <mdiab@mathesonconstructors.com>" to marwandiab8@gmail.com is a forward from his work inbox: treat it as the original message. Don't list the same thread twice.
- Schedule sections: group by sub ("Kolostat", "O'Connor", "Matheson"...) and name the zone or area. For the wrap-up, "Tomorrow" is the next working day; if that isn't ${addDays(dayKey, 1)}, say which day it is.
- Plain, direct language for a superintendent reading on a phone. No emojis, no filler, no advice he didn't ask for.`;
}

/** The model's JSON, held to the section list, with links only from INPUT. */
function parseBriefingJson(raw, kind, allowedLinks) {
  let data = null;
  const text = String(raw || "");
  try { data = JSON.parse(text); } catch (_) {
    const m = /\{[\s\S]*\}/.exec(text);
    if (m) { try { data = JSON.parse(m[0]); } catch (__) { data = null; } }
  }
  if (!data || typeof data !== "object") return null;
  const given = Array.isArray(data.sections) ? data.sections : [];
  const sections = KINDS[kind].sections.map((title, i) => {
    const match = given.find((s) => s && clip(s.title, 80).toLowerCase() === title.toLowerCase()) || given[i] || {};
    const items = (Array.isArray(match.items) ? match.items : [])
      .map((item) => ({
        lead: clip(item && item.lead, 80),
        text: clip(item && item.text, 600),
        link: item && allowedLinks.has(String(item.link || "").trim()) ? String(item.link).trim() : "",
      }))
      .filter((item) => item.lead || item.text)
      .slice(0, 12);
    return { title, items };
  });
  return { headline: clip(data.headline, 300), sections };
}

function linksIn(text) {
  return new Set((String(text).match(/https?:\/\/[^\s"\\]+/g) || []).map((u) => u.trim()));
}

const escapeHtml = (value) =>
  String(value == null ? "" : value).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

/** The report as an email-safe HTML page (everything escaped). */
function renderBriefingHtml({ title, report, writtenAt, appUrl }) {
  const sections = report.sections
    .map((s) => {
      const items = s.items.length
        ? `<ul style="margin:6px 0 14px;padding-left:20px">${s.items
            .map((i) => `<li style="margin:5px 0">${i.lead ? `<b>${escapeHtml(i.lead)}</b>: ` : ""}${escapeHtml(i.text)}${i.link ? ` <a href="${escapeHtml(i.link)}" style="color:#1d4ed8">open</a>` : ""}</li>`)
            .join("")}</ul>`
        : '<p style="margin:6px 0 14px;color:#64748b">Nothing to report.</p>';
      return `<h2 style="font-size:16px;margin:18px 0 4px;color:#0f172a;border-bottom:2px solid #e2e8f0;padding-bottom:3px">${escapeHtml(s.title)}</h2>${items}`;
    })
    .join("");
  return `<!doctype html><html><head><meta charset="utf-8"><title>${escapeHtml(title)}</title></head>
<body style="margin:0;padding:16px;background:#ffffff;font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.45;color:#1e293b;max-width:680px">
<h1 style="font-size:19px;margin:0 0 2px">${escapeHtml(title)}</h1>
<p style="margin:0 0 10px;color:#64748b;font-size:12px">Written ${escapeHtml(writtenAt)} from Gmail, Procore, Todoist, Drive and Site Logistics.${appUrl ? ` <a href="${escapeHtml(appUrl)}" style="color:#1d4ed8">All briefings</a>` : ""}</p>
${report.headline ? `<p style="margin:0 0 6px;font-size:15px"><b>${escapeHtml(report.headline)}</b></p>` : ""}
${sections}
</body></html>`;
}

async function writeReport({ createCompletion, model, kind, dayKey, input }) {
  const params = {
    model,
    response_format: { type: "json_object" },
    messages: [
      { role: "system", content: "You write construction superintendents' daily briefings. You output only valid JSON. No markdown." },
      { role: "user", content: `${promptFor(kind, dayKey)}\n\nINPUT:\n${input}` },
    ],
    max_completion_tokens: 8000,
    temperature: 0.2,
  };
  sanitizeChatCompletionParams(params, model);
  const completion = await createCompletion(params);
  const report = parseBriefingJson(completionText(completion), kind, linksIn(input));
  if (!report) throw new Error("The model's answer was not valid JSON.");
  return report;
}

const json = (res, status, body) => res.status(status).json(body);

/**
 * POST (Bearer token) { kind, dayKey?, windowStart, mail, todoist, procoreInbox, driveFiles } -> 202 { run }.
 * GET ?run=<id> (Bearer token) -> { status: "queued" | "writing" | "ready" | "failed", subject, html, error }.
 */
function createBriefingRequestHandler({ db, FieldValue, logger, token, now = () => new Date() }) {
  return async (req, res) => {
    if (!checkBearer(req.get("authorization"), token())) return json(res, 401, { ok: false, error: "unauthorized" });
    if (req.method === "GET") {
      const runId = String(req.query.run || "");
      if (!/^[\w-]{6,80}$/.test(runId)) return json(res, 400, { ok: false, error: "bad_run" });
      const run = await db.collection(COL_QUEUE).doc(runId).get();
      if (!run.exists) return json(res, 404, { ok: false, error: "not_found" });
      const status = run.get("status") || "queued";
      if (status !== "ready") return json(res, 200, { ok: true, status, error: run.get("error") || "" });
      const report = await db.collection(COL_BRIEFINGS).doc(run.get("reportId")).get();
      return json(res, 200, { ok: true, status, id: report.id, subject: report.get("title"), html: report.get("html") });
    }
    if (req.method !== "POST") return json(res, 405, { ok: false, error: "method_not_allowed" });
    const body = req.body && typeof req.body === "object" ? req.body : {};
    if (!KINDS[body.kind]) return json(res, 400, { ok: false, error: "bad_kind", message: `kind must be one of ${Object.keys(KINDS).join(", ")}` });
    const dayKey = /^\d{4}-\d{2}-\d{2}$/.test(String(body.dayKey || "")) ? body.dayKey : dayKeyIn(now());
    const reportId = `${dayKey}-${body.kind}`;
    const runId = `${reportId}-${now().getTime()}`;
    await db.collection(COL_QUEUE).doc(runId).set({
      status: "queued",
      kind: body.kind,
      dayKey,
      reportId,
      payload: JSON.stringify({
        windowStart: body.windowStart || "",
        mail: Array.isArray(body.mail) ? body.mail : [],
        todoist: body.todoist || null,
        procoreInbox: Array.isArray(body.procoreInbox) ? body.procoreInbox : [],
        driveFiles: Array.isArray(body.driveFiles) ? body.driveFiles : [],
      }).slice(0, 900000),
      createdAt: FieldValue.serverTimestamp(),
    });
    logger.info("docksteaderBriefing queued", { runId, threads: Array.isArray(body.mail) ? body.mail.length : 0 });
    return json(res, 202, { ok: true, run: runId, reportId });
  };
}

/** The queue trigger: writes the report for one queued run. */
async function writeQueuedBriefing({ db, FieldValue, logger, siteLogisticsDb, createCompletion, model, appUrl, runRef, run, now = () => new Date() }) {
  const { kind, dayKey, reportId } = run;
  await runRef.update({ status: "writing" });
  try {
    let site;
    try {
      site = await loadSiteLogistics(siteLogisticsDb, dayKey);
    } catch (err) {
      logger.warn("docksteaderBriefing: Site Logistics not read", { errorMessage: err && err.message });
      site = { note: "Site Logistics could not be read for this report." };
    }
    const at = now();
    const writtenAt = at.toLocaleString("en-CA", { timeZone: TIME_ZONE, weekday: "short", month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).replace(/\./g, "");
    const payload = JSON.parse(run.payload || "{}");
    const input = buildBriefingInput({ kind, dayKey, nowLabel: writtenAt, payload, site });
    const report = await writeReport({ createCompletion, model, kind, dayKey, input });
    const title = `Docksteader ${KINDS[kind].label.toLowerCase()} - ${dayLabel(dayKey)}`;
    const html = renderBriefingHtml({ title, report, writtenAt, appUrl });
    await db.collection(COL_BRIEFINGS).doc(reportId).set({
      memberEmail: MEMBER_EMAIL,
      kind,
      label: KINDS[kind].label,
      dayKey,
      title,
      headline: report.headline,
      sections: report.sections,
      html,
      writtenAt,
      model,
      sources: {
        emailThreads: (payload.mail || []).length,
        todoist: Boolean(payload.todoist && !payload.todoist.error && !payload.todoist.skipped),
        procoreSaved: (payload.procoreInbox || []).length,
        driveFiles: (payload.driveFiles || []).length,
        siteLogistics: !site.note,
      },
      createdAt: FieldValue.serverTimestamp(),
    });
    await runRef.update({ status: "ready", payload: FieldValue.delete(), finishedAt: FieldValue.serverTimestamp() });
    logger.info("docksteaderBriefing written", { reportId, items: report.sections.reduce((n, s) => n + s.items.length, 0) });
  } catch (err) {
    logger.error("docksteaderBriefing failed", { reportId, errorMessage: err && err.message });
    await runRef.update({ status: "failed", error: String((err && err.message) || err).slice(0, 300) });
  }
}

module.exports = {
  COL_BRIEFINGS,
  COL_QUEUE,
  KINDS,
  MEMBER_EMAIL,
  addDays,
  dayKeyIn,
  checkBearer,
  selectSiteData,
  shapeTodoist,
  buildBriefingInput,
  parseBriefingJson,
  renderBriefingHtml,
  createBriefingRequestHandler,
  writeQueuedBriefing,
};
