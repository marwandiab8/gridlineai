const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  elevateProjectAccessWithApprovedMember,
  fallbackInboundIntent,
  formatDurationFromMs,
  inferInboundLogType,
  inferJournalTags,
  isStopTimerCommand,
  looksLikeExplicitAiChatRequest,
  parseNotificationRequest,
  parseStartTimerCommand,
  sanitizeIntentPayload,
  sanitizeRoutePayload,
} = require("./assistant");

test("inferInboundLogType defaults plain updates to construction", () => {
  assert.equal(
    inferInboundLogType("Installed stair nosing at level 2 and patched drywall by elevator."),
    "construction"
  );
});

test("inferInboundLogType routes home and journal text to journal", () => {
  assert.equal(
    inferInboundLogType("Home journal: replaced the faucet and patched the hallway wall."),
    "journal"
  );
});

test("inferInboundLogType recognizes safety and deficiency keywords", () => {
  assert.equal(
    inferInboundLogType("Safety issue guardrail missing at roof edge."),
    "safety"
  );
  assert.equal(
    inferInboundLogType("deficiciency cracked tile at lobby entry."),
    "deficiency"
  );
});

test("sanitizeRoutePayload falls back to construction and preserves all photos", () => {
  const routed = sanitizeRoutePayload(
    {
      title: "Level 3 framing progress",
      description: "Framing crew closed the west corridor bulkheads.",
      tags: ["Framing", "Level-3"],
      requiresFollowUp: false,
    },
    "Framing crew closed the west corridor bulkheads.",
    3
  );

  assert.equal(routed.logType, "construction");
  assert.equal(routed.photos.length, 3);
  assert.deepEqual(routed.photos, ["photo_1", "photo_2", "photo_3"]);
  assert.equal(routed.requiresFollowUp, false);
  assert.ok(routed.tags.includes("construction"));
});

test("fallbackInboundIntent treats journal follow-ups as requests", () => {
  assert.equal(fallbackInboundIntent("continue"), "request");
  assert.equal(fallbackInboundIntent("show me the journal input"), "request");
  assert.equal(fallbackInboundIntent("show me the activities for 2026-04-18"), "request");
});

test("sanitizeIntentPayload falls back safely when AI output is missing", () => {
  const payload = sanitizeIntentPayload({}, "Home journal: picked up groceries and cleaned the kitchen.");
  assert.equal(payload.intent, "journal_entry");
  assert.ok(payload.confidence > 0);
});

test("timer command parsing handles start and stop texts", () => {
  assert.deepEqual(parseStartTimerCommand("start timer for concrete pour"), { label: "concrete pour" });
  assert.deepEqual(parseStartTimerCommand("start timer"), { label: "general task" });
  assert.equal(isStopTimerCommand("stop timer"), true);
  assert.equal(isStopTimerCommand("stop timer now"), true);
  assert.equal(isStopTimerCommand("timer stop"), false);
});

test("formatDurationFromMs renders SMS-friendly duration", () => {
  assert.equal(formatDurationFromMs(4 * 60 * 1000), "4m");
  assert.equal(formatDurationFromMs(60 * 60 * 1000), "1h");
  assert.equal(formatDurationFromMs(74 * 60 * 1000), "1h 14m");
});

test("journal auto-save heuristic distinguishes questions from diary updates", () => {
  assert.equal(looksLikeExplicitAiChatRequest("How should I structure my day?"), true);
  assert.equal(looksLikeExplicitAiChatRequest("Today I feel tired and plan to slow down a bit"), false);
});

test("single-word field prompts stay on the AI request path", () => {
  assert.equal(looksLikeExplicitAiChatRequest("safety"), true);
  assert.equal(looksLikeExplicitAiChatRequest("report"), true);
  assert.equal(looksLikeExplicitAiChatRequest("today"), true);
});

test("inferJournalTags captures feeling and plan cues", () => {
  const tags = inferJournalTags("I feel stressed today but I plan to work on framing and cleanup.");
  assert.ok(tags.includes("journal"));
  assert.ok(tags.includes("feeling"));
  assert.ok(tags.includes("plan"));
  assert.ok(tags.includes("activity"));
});

test("parseNotificationRequest reads management and project audiences", () => {
  assert.deepEqual(
    parseNotificationRequest("inform management that crane delivery moved to 10am", "docksteader"),
    {
      audience: "management",
      messageBody: "crane delivery moved to 10am",
      projectSlug: null,
    }
  );
  assert.deepEqual(
    parseNotificationRequest("notify all users on docksteader that gate 2 is closed", null),
    {
      audience: "project_users",
      messageBody: "gate 2 is closed",
      projectSlug: "docksteader",
    }
  );
  assert.deepEqual(
    parseNotificationRequest("text all users on this project that pour is delayed", "home-site"),
    {
      audience: "project_users",
      messageBody: "pour is delayed",
      projectSlug: "home-site",
    }
  );
});

test("elevateProjectAccessWithApprovedMember honors app-member project access for SMS", () => {
  const projectAccess = {
    exists: true,
    allowed: false,
    reason: "project_not_assigned_to_user",
    projectSlug: "home",
    projectData: { name: "Home" },
  };
  const memberAccess = {
    role: "management",
    allProjects: false,
    projectSlugs: ["home"],
  };

  const elevated = elevateProjectAccessWithApprovedMember(projectAccess, memberAccess);

  assert.equal(elevated.allowed, true);
  assert.equal(elevated.reason, null);
  assert.equal(elevated.accessVia, "approved-phone-app-member");
});

test("elevateProjectAccessWithApprovedMember does not elevate unrelated projects", () => {
  const projectAccess = {
    exists: true,
    allowed: false,
    reason: "project_not_assigned_to_user",
    projectSlug: "home",
  };
  const memberAccess = {
    role: "management",
    allProjects: false,
    projectSlugs: ["docksteader"],
  };

  const elevated = elevateProjectAccessWithApprovedMember(projectAccess, memberAccess);

  assert.equal(elevated.allowed, false);
  assert.equal(elevated.reason, "project_not_assigned_to_user");
});
