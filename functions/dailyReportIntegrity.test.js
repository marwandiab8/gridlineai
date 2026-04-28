/**
 * Regression: Docksteader report must not include Home chatter, meta SMS, or cross-linked media.
 * Run: npm test (from functions/) or node --test dailyReportIntegrity.test.js
 */

const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  filterLogEntriesForProjectDailyReport,
  filterMediaForProjectDailyReport,
  promoteFieldReportSections,
  isMetaOrControlChatter,
  entryIsExcludedFromReport,
  curateFieldEntriesForDailyReport,
  isValidTradeHeading,
} = require("./dailyReportIntegrity");
const {
  mergeStructuredDailyReportJson,
  extractStructuredTableOverrides,
} = require("./dailyReportContent");
const { normalizeReportLineText } = require("./dailyReportBulkText");
const { filterEntriesForDailySummary } = require("./logEntryRepository");

const dock = "docksteader";
const home = "home";

function entry(id, slug, text, cat = "journal", include = true, sourceMessageId = null) {
  return {
    id,
    projectSlug: slug,
    category: cat,
    rawText: text,
    normalizedText: text,
    includeInDailySummary: include,
    sourceMessageId: sourceMessageId || null,
    dailySummarySections: ["dayLog", "journal"],
  };
}

test("filler and report-request phrases are excluded", () => {
  assert.equal(isMetaOrControlChatter("No field updates provided", {}), true);
  assert.equal(isMetaOrControlChatter("No details provided", {}), true);
  assert.equal(isMetaOrControlChatter("Please send the daily report by text", {}), true);
  assert.equal(isMetaOrControlChatter("This PDF format is terrible", {}), true);
});

test("entry excluded if any text layer is meta", () => {
  const e = {
    id: "x",
    projectSlug: dock,
    rawText: "continue",
    normalizedText: "continue",
    summaryText: "Pour at grid 5 completed",
    includeInDailySummary: true,
  };
  assert.equal(entryIsExcludedFromReport(e), true);
});

test("meta and control lines are detected", () => {
  assert.equal(isMetaOrControlChatter("daily report", {}), true);
  assert.equal(isMetaOrControlChatter("Continue", {}), true);
  assert.equal(isMetaOrControlChatter("Show me the pictures for home", {}), true);
  assert.equal(isMetaOrControlChatter("Awaiting SMS input to generate field log entry", {}), true);
  assert.equal(isMetaOrControlChatter("Add those photo to a project named home", {}), true);
  assert.equal(isMetaOrControlChatter("Switch to project docksteader", {}), true);
  assert.equal(isMetaOrControlChatter("Open the home photo gallery please", {}), true);
  assert.equal(isMetaOrControlChatter("This PDF report doesn't include my pour notes", {}), true);
  assert.equal(isMetaOrControlChatter("Tap below to download the report", {}), true);
});

test("report-editing chatter is excluded from report content", () => {
  assert.equal(isMetaOrControlChatter("Fix this in the report before sending the PDF", {}), true);
  assert.equal(isMetaOrControlChatter("Include this photo in the report", {}), true);
  assert.equal(isMetaOrControlChatter("Make sure this picture is in the PDF report", {}), true);
  assert.equal(isMetaOrControlChatter("Change the title spacing on the report", {}), true);
});

test("field notes with show photos wording stay reportable", () => {
  assert.equal(
    isMetaOrControlChatter("Show me the photos from grid GL-5 after the pour", {}),
    false
  );
});

test("Docksteader report excludes Home-tagged entries and meta", () => {
  const raw = [
    entry("1", dock, "Roofing installing BSW blindside waterproofing at piers K-5.1", "journal"),
    entry("2", home, "Add those photo to a project named home", "journal"),
    entry("3", dock, "Show me the pictures for home", "journal"),
    entry("4", dock, "daily report", "journal"),
    entry("5", dock, "Continue", "journal"),
    entry("6", home, "Personal note at home, kids visit", "journal"),
    entry("7", dock, "At home tonight with family dinner, not on site", "journal"),
  ];
  const filtered = filterLogEntriesForProjectDailyReport(raw, dock);
  const ids = new Set(filtered.map((e) => e.id));
  assert.equal(ids.has("1"), true);
  assert.equal(ids.has("2"), false);
  assert.equal(ids.has("3"), false);
  assert.equal(ids.has("4"), false);
  assert.equal(ids.has("5"), false);
  assert.equal(ids.has("6"), false);
  assert.equal(ids.has("7"), false);
});

test("project-scoped report includes legacy unassigned entry with explicit project prefix", () => {
  const raw = [
    entry(
      "legacy-1",
      null,
      "Project: docksteader\nALC assign 11 crew to work on GL K for footings including 2 rebar crew",
      "journal"
    ),
  ];
  const filtered = filterLogEntriesForProjectDailyReport(raw, dock);
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].id, "legacy-1");
});

test("project-scoped report excludes entry explicitly labeled for another project", () => {
  const raw = [
    entry(
      "wrong-1",
      dock,
      "Project: home\nHome update accidentally tagged under docksteader.",
      "journal"
    ),
  ];
  const filtered = filterLogEntriesForProjectDailyReport(raw, dock);
  assert.equal(filtered.length, 0);
});

test("media excluded when projectId wrong or link outside report entry set", () => {
  const allowed = new Set(["a1"]);
  const media = [
    { id: "m1", storagePath: "p", projectId: dock, linkedLogEntryId: "a1" },
    { id: "m2", storagePath: "p", projectId: home, linkedLogEntryId: null },
    { id: "m3", storagePath: "p", projectId: dock, linkedLogEntryId: "orphan" },
    {
      id: "m4",
      storagePath: "p",
      projectId: dock,
      linkedLogEntryId: null,
      sourceMessageId: "orphan-sms",
      captionText: null,
    },
  ];
  const out = filterMediaForProjectDailyReport(media, dock, allowed, {
    allowedSourceMessageIds: new Set(["sms-for-a1"]),
  });
  assert.equal(out.length, 1);
  assert.equal(out[0].id, "m1");
});

test("unlinked media included via sourceMessageId or reportable caption or explicit flag", () => {
  const allowed = new Set(["e1"]);
  const base = { storagePath: "p", projectId: dock, linkedLogEntryId: null };
  const byMsg = filterMediaForProjectDailyReport(
    [{ id: "a", ...base, sourceMessageId: "in-report", captionText: "" }],
    dock,
    allowed,
    { allowedSourceMessageIds: new Set(["in-report"]) }
  );
  assert.equal(byMsg.length, 1);

  const byCaption = filterMediaForProjectDailyReport(
    [
      {
        id: "b",
        ...base,
        sourceMessageId: "random",
        captionText: "Log deficiency: water stain at east stair soffit",
      },
    ],
    dock,
    allowed,
    { allowedSourceMessageIds: new Set() }
  );
  assert.equal(byCaption.length, 1);

  const byFlag = filterMediaForProjectDailyReport(
    [{ id: "c", ...base, includeInDailyReport: true, captionText: "" }],
    dock,
    allowed,
    { allowedSourceMessageIds: new Set() }
  );
  assert.equal(byFlag.length, 1);
});

test("Docksteader regression: mis-tagged unlinked Home photo and meta lines never reach report", () => {
  const raw = [
    entry("e1", dock, "Pour completed at grid 5 east — 40 cy placed", "journal", true, "sms-dock-1"),
    entry("e2", dock, "Open the home photo gallery please", "journal", true, "sms-gallery"),
    entry("e3", dock, "This daily report doesn't include my notes from today", "journal", true, "sms-bad-report"),
    entry("e4", home, "Personal weekend note with family", "journal", true, "sms-home-journal"),
  ];
  const summarized = filterEntriesForDailySummary(raw);
  const logForPdf = filterLogEntriesForProjectDailyReport(summarized, dock);
  assert.equal(logForPdf.length, 1);
  assert.equal(logForPdf[0].id, "e1");

  const entryIdSet = new Set(logForPdf.map((e) => e.id));
  const allowedSourceMessageIds = new Set(
    logForPdf
      .map((e) => e.sourceMessageId)
      .filter(Boolean)
      .map(String)
  );
  assert.ok(allowedSourceMessageIds.has("sms-dock-1"));

  const media = [
    {
      id: "m-home-wrong-tag",
      storagePath: "projects/docksteader/media/x/sid/img.jpg",
      projectId: dock,
      linkedLogEntryId: null,
      sourceMessageId: "sms-home-mms",
      captionText: "",
    },
    {
      id: "m-home-wrong-caption",
      storagePath: "projects/docksteader/media/x/sid2/img.jpg",
      projectId: dock,
      linkedLogEntryId: null,
      sourceMessageId: "sms-home-mms-2",
      captionText: "Family BBQ at the lake",
    },
    {
      id: "m-ok-linked",
      storagePath: "projects/docksteader/media/x/sid3/img.jpg",
      projectId: dock,
      linkedLogEntryId: "e1",
      sourceMessageId: "sms-dock-1",
      captionText: "",
    },
  ];
  const mediaOut = filterMediaForProjectDailyReport(media, dock, entryIdSet, {
    allowedSourceMessageIds,
  });
  const mediaIds = new Set(mediaOut.map((m) => m.id));
  assert.equal(mediaOut.length, 1);
  assert.ok(mediaIds.has("m-ok-linked"));
  assert.ok(!mediaIds.has("m-home-wrong-tag"));
  assert.ok(!mediaIds.has("m-home-wrong-caption"));
});

test("promotion adds weather/work hints for journal field lines", () => {
  const rows = promoteFieldReportSections([
    entry("w", dock, "Weather: high 4°C, low -3°C.", "journal"),
    entry("x", dock, "Coreydale excavated the sacrificial hole.", "journal"),
  ]);
  assert.ok(rows[0].dailySummarySections.includes("weather"));
  assert.ok(rows[1].dailySummarySections.includes("workInProgress"));
});

test("curated field entries drop manpower clarification and keyword chatter", () => {
  const raw = [
    entry("1", dock, "Roofing installing BSW blindside waterproofing at piers K-5.1", "journal"),
    entry("2", dock, "Manpower update requested for Docksteader project.", "journal"),
    entry("3", dock, "Clarifies manpower report is for today, not yesterday", "journal"),
    entry("4", dock, "Give me a list of keywords to use with ChatGPT", "journal"),
    entry("5", dock, "Request to continue previous conversation", "journal"),
    entry("6", dock, "No message content provided to analyze.", "journal"),
  ];
  const afterProject = filterLogEntriesForProjectDailyReport(raw, dock);
  const curated = curateFieldEntriesForDailyReport(afterProject);
  assert.equal(curated.length, 1);
  assert.equal(curated[0].id, "1");
});

test("isValidTradeHeading rejects junk verbs and accepts real trades", () => {
  assert.equal(isValidTradeHeading("Add"), false);
  assert.equal(isValidTradeHeading("Request"), false);
  assert.equal(isValidTradeHeading("Roofing"), true);
  assert.equal(isValidTradeHeading("GC"), true);
});

test("mergeStructuredDailyReportJson enables structured layout when AI sections exist", () => {
  const det = {
    weatherToday: "Not stated in field messages.",
    manpowerNarrative: "—",
    issuesText: "—",
    inspectionText: "—",
    concreteNarrativeBlock: "—",
    workNarrativeBlock: "deterministic work",
  };
  const merged = mergeStructuredDailyReportJson(
    det,
    {
      executiveSummary: "Summary.",
      weather: { todaySummary: "", weeklyForecastRows: [] },
      manpower: { rows: [], summaryNote: "" },
      workCompletedInProgress: {
        sections: [{ trade: "Roofing", items: ["Installed membrane at K-5."] }],
      },
      issuesDeficienciesDelays: { items: [] },
      inspections: { items: [] },
      concreteSummary: { rows: [], narrativeNote: "" },
      openItems: { rows: [], narrativeNote: "" },
    },
    new Date("2026-03-28T12:00:00Z")
  );
  assert.equal(merged.useStructuredWorkLayout, true);
  assert.equal(merged.workSectionsAi.length, 1);
  assert.equal(merged.workSectionsAi[0].trade, "Roofing");
});

test("mergeStructuredDailyReportJson disables structured layout without work sections", () => {
  const det = {
    weatherToday: "x",
    manpowerNarrative: "m",
    issuesText: "i",
    inspectionText: "in",
    concreteNarrativeBlock: "c",
    workNarrativeBlock: "wk body",
  };
  const merged = mergeStructuredDailyReportJson(
    det,
    {
      executiveSummary: "",
      weather: {},
      manpower: {},
      workCompletedInProgress: { sections: [] },
      issuesDeficienciesDelays: { items: [] },
      inspections: { items: [] },
      concreteSummary: {},
      openItems: {},
    },
    new Date("2026-03-28T12:00:00Z")
  );
  assert.equal(merged.useStructuredWorkLayout, false);
  assert.equal(merged.workNarrative.includes("wk body"), true);
});

test("extractStructuredTableOverrides maps manpower and concrete rows", () => {
  const o = extractStructuredTableOverrides({
    manpower: {
      rows: [
        { trade: "ALC", foreman: "A", workers: "7", notes: "—" },
      ],
    },
    concreteSummary: {
      rows: [{ location: "Grid 5", volume: "40 cy", status: "Placed" }],
    },
  });
  assert.equal(o.manpowerRows.length, 1);
  assert.equal(o.manpowerRows[0][0], "ALC");
  assert.equal(o.concreteRows.length, 1);
  assert.equal(o.concreteRows[0][0], "Grid 5");
});

test("normalizeReportLineText strips bulk ingestion lines and mismatched embedded dates", () => {
  const raw =
    "Add the below updates to the Docksteader project.\nProject Docksteader Monday April 06 2026\nALC 7 workers on south toe.";
  const out = normalizeReportLineText(raw, "2026-04-07");
  assert.ok(!/Add the below/i.test(out));
  assert.ok(!/April 06/i.test(out));
  assert.ok(/ALC\s+7/i.test(out));
});

test("unlinked media with drone caption is reportable for daily PDF", () => {
  const allowed = new Set(["e1"]);
  const media = [
    {
      id: "drone1",
      storagePath: "projects/docksteader/media/x/img.jpg",
      projectId: dock,
      linkedLogEntryId: null,
      sourceMessageId: "orphan-sms",
      captionText: "Drone orthophoto — north elevation progress",
    },
  ];
  const out = filterMediaForProjectDailyReport(media, dock, allowed, {
    allowedSourceMessageIds: new Set(),
  });
  assert.equal(out.length, 1);
  assert.equal(out[0].id, "drone1");
});
