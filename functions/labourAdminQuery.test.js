const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  ADMIN_LABOUR_DENIAL_TEXT,
  aggregateCanonicalLabour,
  buildAdminLabourArtifactIdentity,
  buildLabourSmsRequestKey,
  canonicalizeLabourEntries,
  executeAdminLabourQuery,
  formatAdminLabourText,
  formatHours,
  getAdminLabourAccessDecision,
  isAdministratorLabourAccess,
  normalizeProjectRegistry,
  normalizeWorkerRegistry,
  parseAdminLabourQuery,
  prepareAdminLabourQuery,
  resolveDateRange,
  torontoDateKey,
} = require("./labourAdminQuery");
const { buildAdminLabourPdfModel } = require("./labourAdminReportPdf");
const { parseLabourHoursCommand } = require("./labourRepository");

const NOW = new Date("2026-08-17T16:00:00.000Z");
const PROJECT_ROWS = [
  { id: "docksteader", name: "Docksteader", active: true },
  { id: "other-project", name: "Other Project", active: true, aliases: ["Other"] },
  { id: "home", slug: "home", name: "Home", active: true },
];
const WORKER_ROWS = [
  { id: "+12890000001", name: "Ethan Garth", active: true, aliases: ["E. Garth"] },
  { id: "+12893385196", name: "Shawn Jones", active: true, previousNames: ["Shawn J"] },
  { id: "+12890000003", name: "Alex Smith", active: true },
  { id: "+12890000004", name: "Alex Brown", active: true },
];

function parseAndPrepare(text, overrides = {}) {
  const parsed = parseAdminLabourQuery(text);
  assert.ok(parsed, text);
  return prepareAdminLabourQuery({
    parsed: { ...parsed, queryText: text },
    projectRows: overrides.projectRows || PROJECT_ROWS,
    workerRows: overrides.workerRows || WORKER_ROWS,
    now: overrides.now || NOW,
    payPeriodConfig: Object.prototype.hasOwnProperty.call(overrides, "payPeriodConfig")
      ? overrides.payPeriodConfig
      : undefined,
  });
}

test("parses the required administrator labour questions deterministically", () => {
  const cases = [
    ["How many labour hours for the Docksteader job?", "total", "text", "Docksteader", ""],
    ["How many hours for the current pay period for all labourers?", "total", "text", "", ""],
    ["How many hours did Ethan log?", "total", "text", "", "Ethan"],
    ["How many hours did Ethan log on 2026-08-06?", "total", "text", "", "Ethan"],
    ["Who has hours on 2026-08-06?", "who", "text", "", ""],
    ["How many total labour hours were entered on 2026-08-06?", "total", "text", "", ""],
    ["How many hours did Shawn Jones work from 2026-08-12 to 2026-08-14?", "total", "text", "", "Shawn Jones"],
    ["Send me a labour report for Docksteader.", "report", "pdf", "Docksteader", ""],
    ["Send me a PDF labour report for Docksteader for the current pay period.", "report", "pdf", "Docksteader", ""],
    ["Labour report Docksteader 2026-08-01 to 2026-08-15.", "report", "pdf", "Docksteader", ""],
    ["Labour help", "help", "text", "", ""],
  ];
  for (const [text, intent, output, projectHint, workerHint] of cases) {
    const parsed = parseAdminLabourQuery(text);
    assert.ok(parsed, text);
    assert.equal(parsed.intent, intent, text);
    assert.equal(parsed.output, output, text);
    assert.equal(parsed.projectHint, projectHint, text);
    assert.equal(parsed.workerHint, workerHint, text);
    assert.equal(parsed.confidence, 1, text);
  }
});

test("accepts punctuation, extra spaces, lowercase, labour spelling variants, and compact report wording", () => {
  for (const text of [
    "  how many   labor hours for the docksteader job!!! ",
    "LABOUR REPORT DOCKSTEADER 2026-08-01 TO 2026-08-15",
    "send labour report for docksteader",
    "who has labour hours on 2026-08-06;",
  ]) {
    assert.ok(parseAdminLabourQuery(text), text);
  }
});

test("preserves normal labour submissions and Shawn compact-duration formats", () => {
  assert.equal(parseAdminLabourQuery("8.5 hours housekeeping and cleanup"), null);
  assert.equal(parseLabourHoursCommand("8.5 hours housekeeping and cleanup").hours, 8.5);
  const shawn = parseLabourHoursCommand(
    "8.5 hours -2.5 hrs site cleaning - 2 hrs relocating material -3 hrs unloading deliveries -1 hr digging out gravel to expose footings for column install"
  );
  assert.equal(shawn.hours, 8.5);
  assert.equal(
    shawn.workOn,
    "2.5h site cleaning - 2h relocating material - 3h unloading deliveries - 1h digging out gravel to expose footings for column install"
  );
});

test("requires the canonical active admin role", () => {
  assert.equal(isAdministratorLabourAccess({ role: "admin", memberDocId: "admin@example.test" }), true);
  assert.equal(isAdministratorLabourAccess({ role: "management", memberDocId: "manager@example.test" }), false);
  assert.equal(isAdministratorLabourAccess({ role: "viewer", memberDocId: "viewer@example.test" }), false);
  assert.equal(isAdministratorLabourAccess(null), false);
});

test("unauthorized cross-worker and project queries return only the generic denial", () => {
  const managementAccess = { role: "management", memberDocId: "manager@example.test" };
  for (const text of [
    "How many hours did Ethan log on 2026-08-06?",
    "How many labour hours for the Docksteader job?",
    "Who has hours on 2026-08-06?",
    "Send me a labour report for Docksteader.",
    "Labour help",
  ]) {
    assert.equal(getAdminLabourAccessDecision({
      access: managementAccess,
      parsed: parseAdminLabourQuery(text),
      text,
    }), "denied", text);
  }
  assert.equal(getAdminLabourAccessDecision({
    access: managementAccess,
    parsed: parseAdminLabourQuery("How many hours this week?"),
    text: "How many hours this week?",
  }), "legacy_self_service");
  assert.equal(ADMIN_LABOUR_DENIAL_TEXT, "This labour query is available only to an authorized GridlineAI administrator.");
  assert.doesNotMatch(ADMIN_LABOUR_DENIAL_TEXT, /Ethan|Docksteader|worker|project/i);
});

test("resolves Docksteader exactly and uses project-to-date for a project-only total", () => {
  const prepared = parseAndPrepare("How many labour hours for the Docksteader job?");
  assert.equal(prepared.status, "ready");
  assert.equal(prepared.request.projectSlug, "docksteader");
  assert.equal(prepared.request.periodKind, "project_to_date");
  assert.equal(prepared.request.periodLabel, "Project-to-date");
});

test("defaults worker totals and PDFs to the authoritative current pay period", () => {
  const worker = parseAndPrepare("How many hours did Ethan log?");
  assert.equal(worker.status, "ready");
  assert.equal(worker.request.workerName, "Ethan Garth");
  assert.equal(worker.request.startKey, "2026-08-15");
  assert.equal(worker.request.endKey, "2026-08-28");

  const report = parseAndPrepare("Send me a labour report for Docksteader.");
  assert.equal(report.request.startKey, "2026-08-15");
  assert.equal(report.request.endKey, "2026-08-28");
});

test("fails safely when pay-period configuration is absent", () => {
  const parsed = parseAdminLabourQuery("How many hours did Ethan log?");
  const prepared = prepareAdminLabourQuery({
    parsed: { ...parsed, queryText: "How many hours did Ethan log?" },
    projectRows: PROJECT_ROWS,
    workerRows: WORKER_ROWS,
    now: NOW,
    payPeriodConfig: null,
  });
  assert.equal(prepared.status, "clarification");
  assert.equal(prepared.reason, "pay_period_not_configured");
});

test("resolves exact dates and inclusive ranges", () => {
  const exact = parseAndPrepare("How many hours did Ethan log on 2026-08-06?");
  assert.equal(exact.request.startKey, "2026-08-06");
  assert.equal(exact.request.endKey, "2026-08-06");
  const range = parseAndPrepare("How many hours did Shawn Jones work from 2026-08-12 to 2026-08-14?");
  assert.equal(range.request.startKey, "2026-08-12");
  assert.equal(range.request.endKey, "2026-08-14");
});

test("uses Toronto calendar boundaries across daylight-saving transitions", () => {
  assert.equal(torontoDateKey(new Date("2026-03-08T04:30:00.000Z")), "2026-03-07");
  assert.equal(torontoDateKey(new Date("2026-03-08T05:30:00.000Z")), "2026-03-08");
  assert.equal(torontoDateKey(new Date("2026-11-01T03:30:00.000Z")), "2026-10-31");
  assert.equal(torontoDateKey(new Date("2026-11-01T04:30:00.000Z")), "2026-11-01");
});

test("asks for clarification for ambiguous workers and never combines shared first names", () => {
  const prepared = parseAndPrepare("How many hours did Alex log?");
  assert.equal(prepared.status, "clarification");
  assert.equal(prepared.reason, "worker_ambiguous");
  assert.deepEqual(prepared.candidates, ["Alex Brown", "Alex Smith"]);
});

test("asks for clarification for ambiguous project aliases", () => {
  const projectRows = [
    { id: "north-a", name: "North A", aliases: ["North"] },
    { id: "north-b", name: "North B", aliases: ["North"] },
  ];
  const prepared = parseAndPrepare("How many labour hours for the North project?", { projectRows });
  assert.equal(prepared.status, "clarification");
  assert.equal(prepared.reason, "project_ambiguous");
});

test("Home must be explicitly labour-enabled and is excluded from all-work reports", () => {
  const home = parseAndPrepare("How many labour hours for the Home project?");
  assert.equal(home.status, "clarification");
  assert.equal(home.reason, "project_not_labour_enabled");
  const enabledHome = parseAndPrepare("How many labour hours for the Home project?", {
    projectRows: PROJECT_ROWS.map((row) => row.id === "home" ? { ...row, labourEnabled: true } : row),
  });
  assert.equal(enabledHome.status, "ready");
  assert.equal(enabledHome.request.projectSlug, "home");
});

function canonicalFixture(request) {
  const projects = normalizeProjectRegistry(PROJECT_ROWS);
  const workers = normalizeWorkerRegistry(WORKER_ROWS);
  const entries = [
    { id: "dock-ethan", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12890000001", labourerName: "Old Ethan Name", minutesWorked: 510 },
    { id: "dock-shawn", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12893385196", minutesWorked: 480 },
    { id: "other-ethan", reportDateKey: "2026-08-06", projectSlug: "other-project", labourerPhone: "+12890000001", minutesWorked: 120 },
    { id: "home-private-marker", reportDateKey: "2026-08-06", projectSlug: "home", labourerPhone: "+12890000001", minutesWorked: 600 },
    { id: "unassigned", reportDateKey: "2026-08-06", labourerPhone: "+12890000001", minutesWorked: 60 },
    { id: "contradictory", reportDateKey: "2026-08-06", projectSlug: "home", projectId: "docksteader", labourerPhone: "+12890000001", minutesWorked: 60 },
    { id: "invalid-minutes", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12890000001", minutesWorked: 90.5 },
    { id: "both-fields", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12890000001", minutesWorked: 60, hours: 99 },
    { id: "legacy", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12893385196", hours: 1.5 },
    { id: "deleted", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12893385196", minutesWorked: 60, deleted: true },
    { id: "test", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12893385196", minutesWorked: 60, source: "test" },
  ];
  const canonical = canonicalizeLabourEntries({ entries, projects, workers, request });
  return aggregateCanonicalLabour({ canonical, request });
}

test("canonical aggregation isolates Home and Docksteader before summing", () => {
  const request = {
    intent: "total",
    projectSlug: "docksteader",
    projectName: "Docksteader",
    startKey: "2026-08-06",
    endKey: "2026-08-06",
    periodLabel: "2026-08-06",
  };
  const result = canonicalFixture(request);
  assert.equal(result.projectCount, 1);
  assert.equal(result.sections[0].projectSlug, "docksteader");
  assert.equal(result.documentIds.includes("home-private-marker"), false);
  assert.equal(result.totalMinutes, 510 + 480 + 60 + 90);
});

test("all-work aggregation keeps validated projects in separate sections", () => {
  const request = {
    intent: "total",
    projectSlug: null,
    projectName: null,
    allWorkProjects: true,
    startKey: "2026-08-06",
    endKey: "2026-08-06",
    periodLabel: "2026-08-06",
  };
  const result = canonicalFixture(request);
  assert.deepEqual(result.sections.map((section) => section.projectSlug), ["docksteader", "other-project"]);
  assert.equal(result.documentIds.includes("home-private-marker"), false);
  assert.equal(result.totalMinutes, 510 + 480 + 120 + 60 + 90);
});

test("explicit all-project scope cannot collapse to a project name mentioned in the wording", () => {
  const text = "How many labour hours for all projects, including Docksteader?";
  const parsed = parseAdminLabourQuery(text);
  const prepared = prepareAdminLabourQuery({
    parsed: { ...parsed, queryText: text },
    projectRows: PROJECT_ROWS,
    workerRows: WORKER_ROWS,
    now: NOW,
  });
  assert.equal(prepared.status, "ready");
  assert.equal(prepared.request.projectSlug, null);
  assert.equal(prepared.request.allWorkProjects, true);
});

test("open-ended all-project audit ranges use the earliest date across every project", () => {
  const projects = normalizeProjectRegistry(PROJECT_ROWS);
  const workers = normalizeWorkerRegistry(WORKER_ROWS);
  const request = {
    intent: "total",
    projectSlug: null,
    startKey: null,
    endKey: "2026-08-17",
    periodKind: "all_time",
    periodLabel: "All time",
  };
  const canonical = canonicalizeLabourEntries({
    entries: [
      { id: "dock-later", reportDateKey: "2026-08-10", projectSlug: "docksteader", labourerPhone: "+12890000001", minutesWorked: 60 },
      { id: "other-earlier", reportDateKey: "2026-07-01", projectSlug: "other-project", labourerPhone: "+12890000001", minutesWorked: 60 },
    ],
    projects,
    workers,
    request,
  });
  const result = aggregateCanonicalLabour({ canonical, request });
  assert.equal(result.request.startKey, "2026-07-01");
});

test("minutesWorked is authoritative and legacy hours is never added", () => {
  const request = {
    intent: "total",
    projectSlug: "docksteader",
    startKey: "2026-08-06",
    endKey: "2026-08-06",
  };
  const result = canonicalFixture(request);
  const both = result.entries.find((entry) => entry.id === "both-fields");
  assert.equal(both.minutesWorked, 60);
  assert.equal(result.auditFlags.contradictory_legacy_hours_ignored, 1);
  assert.match(formatAdminLabourText(result), /conflicting legacy hours value was ignored/);
  assert.equal(result.entries.find((entry) => entry.id === "legacy").minutesWorked, 90);
});

test("invalid, deleted, test, unassigned, and contradictory records are excluded with reasons", () => {
  const result = canonicalFixture({
    intent: "total",
    projectSlug: null,
    startKey: "2026-08-06",
    endKey: "2026-08-06",
  });
  assert.equal(result.excludedReasons.invalid_minutes, 1);
  assert.equal(result.excludedReasons.deleted, 1);
  assert.equal(result.excludedReasons.test_source, 1);
  assert.equal(result.excludedReasons.missing_project_ownership, 1);
  assert.equal(result.excludedReasons.contradictory_project_ownership, 1);
  assert.equal(result.excludedReasons.project_not_labour_enabled, 1);
});

test("worker rename and alias resolution use one canonical worker", () => {
  const projects = normalizeProjectRegistry(PROJECT_ROWS);
  const workers = normalizeWorkerRegistry(WORKER_ROWS);
  const canonical = canonicalizeLabourEntries({
    entries: [
      { id: "renamed-by-phone", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerPhone: "+12890000001", labourerName: "Former Name", minutesWorked: 300 },
      { id: "legacy-alias", reportDateKey: "2026-08-06", projectSlug: "docksteader", labourerName: "E. Garth", hours: 2 },
    ],
    projects,
    workers,
    request: { projectSlug: "docksteader", startKey: "2026-08-06", endKey: "2026-08-06" },
  });
  const result = aggregateCanonicalLabour({ canonical, request: { projectSlug: "docksteader", startKey: "2026-08-06", endKey: "2026-08-06" } });
  assert.equal(result.workerCount, 1);
  assert.equal(result.sections[0].workerTotals[0].workerName, "Ethan Garth");
  assert.equal(result.totalMinutes, 420);
});

test("malformed worker identifiers cannot become pseudo-phone identities", () => {
  assert.deepEqual(
    normalizeWorkerRegistry([{ id: "worker-1", name: "Malformed Identifier" }]),
    []
  );
  assert.equal(
    normalizeWorkerRegistry([{ id: "+442079460123", name: "International Worker" }])[0].workerId,
    "+442079460123"
  );
});

test("inclusive Shawn canary range totals exactly 1,020 minutes and 17 hours", () => {
  const projects = normalizeProjectRegistry(PROJECT_ROWS);
  const workers = normalizeWorkerRegistry(WORKER_ROWS);
  const request = {
    intent: "total",
    projectSlug: "docksteader",
    workerId: "+12893385196",
    startKey: "2026-08-12",
    endKey: "2026-08-14",
    periodLabel: "2026-08-12 to 2026-08-14",
  };
  const canonical = canonicalizeLabourEntries({
    entries: [
      { id: "ZiDFceInJuQwjOubIrcv", reportDateKey: "2026-08-12", projectSlug: "docksteader", labourerPhone: "+12893385196", minutesWorked: 510 },
      { id: "KdrDHiv649TYE5uo1gPc", reportDateKey: "2026-08-14", projectSlug: "docksteader", labourerPhone: "+12893385196", minutesWorked: 510 },
      { id: "outside-range", reportDateKey: "2026-08-15", projectSlug: "docksteader", labourerPhone: "+12893385196", minutesWorked: 510 },
    ],
    projects,
    workers,
    request,
  });
  const result = aggregateCanonicalLabour({ canonical, request });
  assert.equal(result.totalMinutes, 1020);
  assert.equal(formatHours(result.totalMinutes), "17");
  assert.deepEqual(result.documentIds, ["ZiDFceInJuQwjOubIrcv", "KdrDHiv649TYE5uo1gPc"]);
});

test("text and PDF models use the identical canonical totals", () => {
  const result = canonicalFixture({
    intent: "total",
    projectSlug: "docksteader",
    projectName: "Docksteader",
    startKey: "2026-08-06",
    endKey: "2026-08-06",
    periodLabel: "2026-08-06",
  });
  const text = formatAdminLabourText(result);
  const pdf = buildAdminLabourPdfModel(result, NOW);
  assert.match(text, new RegExp(`${formatHours(result.totalMinutes)} labour hours`));
  assert.equal(pdf.totalMinutes, result.totalMinutes);
  assert.equal(pdf.totalHours, formatHours(result.totalMinutes));
  assert.equal(pdf.sections[0].totalMinutes, result.sections[0].totalMinutes);
});

test("zero results are explicit and invalid exclusions produce a warning", () => {
  const empty = aggregateCanonicalLabour({
    canonical: { included: [], excludedCount: 0, excludedReasons: {}, auditFlags: {} },
    request: {
      intent: "total",
      workerName: "Ethan Garth",
      startKey: "2026-08-06",
      endKey: "2026-08-06",
      periodLabel: "2026-08-06",
    },
  });
  assert.match(formatAdminLabourText(empty), /^No matching labour entries were found/);
  const warned = { ...empty, excludedCount: 2 };
  assert.match(formatAdminLabourText(warned), /Warning: 2 invalid entries were excluded/);
});

test("long worker breakdowns switch to a concise response before SMS truncation", () => {
  const text = formatAdminLabourText({
    request: {
      intent: "who",
      startKey: "2026-08-06",
      endKey: "2026-08-06",
    },
    sections: [{
      projectName: "Docksteader",
      totalMinutes: 3600,
      workerTotals: Array.from({ length: 30 }, (_, index) => ({
        workerName: `Distinct Worker Marker ${String(index + 1).padStart(2, "0")}`,
        totalMinutes: 120,
      })),
    }],
    totalMinutes: 3600,
    workerCount: 30,
    entryCount: 30,
    excludedCount: 0,
  });
  assert.ok(text.length <= 460);
  assert.match(text, /Reply with a labour report request/);
  assert.doesNotMatch(text, /Distinct Worker Marker/);
});

test("database failures reject rather than becoming a zero total", async () => {
  const db = {
    collection() {
      return {
        async get() {
          throw new Error("database unavailable");
        },
      };
    },
  };
  await assert.rejects(
    executeAdminLabourQuery({
      db,
      parsed: { ...parseAdminLabourQuery("Who has hours on 2026-08-06?"), queryText: "Who has hours on 2026-08-06?" },
      now: NOW,
    }),
    /database unavailable/
  );
});

test("MessageSid-derived artifacts are stable and do not expose the SID", () => {
  const key = buildLabourSmsRequestKey("SM00000000000000000000000000000000");
  assert.equal(key.length, 40);
  const first = buildAdminLabourArtifactIdentity(key);
  const second = buildAdminLabourArtifactIdentity(key);
  assert.deepEqual(first, second);
  assert.doesNotMatch(JSON.stringify(first), /SM0000/);
  assert.match(first.storagePath, /^adminLabourReports\/[a-f0-9]{40}\.pdf$/);
  assert.doesNotMatch(first.storagePath, /^labourReports\//);
});

test("relative period resolver is deterministic", () => {
  const current = resolveDateRange({
    period: { kind: "current_pay_period", explicit: true },
    now: NOW,
  });
  assert.deepEqual(
    { startKey: current.startKey, endKey: current.endKey },
    { startKey: "2026-08-15", endKey: "2026-08-28" }
  );
});
