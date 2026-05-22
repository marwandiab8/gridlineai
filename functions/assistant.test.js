const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  elevateProjectAccessWithApprovedMember,
  fallbackInboundIntent,
  formatDurationFromMs,
  inferInboundLogType,
  inferJournalTags,
  decideFallbackRouting,
  applyManpowerCorrectionToEntry,
  applyNarrativeCorrectionToEntry,
  isExplicitLabourBalanceText,
  isExplicitLabourEntryText,
  isAffirmativeCorrectionFollowUp,
  isStopTimerCommand,
  looksLikeCorrectionPrompt,
  looksLikeAssistantFollowUpAnswer,
  looksLikeExplicitAiChatRequest,
  looksLikeNarrativeSaveCandidate,
  buildRecentCorrectionDateKeys,
  parseLookaheadActivitiesQuery,
  parseNotificationRequest,
  parseNarrativeCorrectionCommand,
  parseHomeTodoCommand,
  parseTodoListRequest,
  parseTodoDateTimeInput,
  parseTodoReportRequest,
  parseStartTimerCommand,
  isExplicitProjectSetRequest,
  sanitizeAssistantActionPlan,
  sanitizeIntentPayload,
  sanitizeRoutePayload,
  normalizePendingTodoDraft,
  getNextMissingTodoField,
  shouldTrackAssistantFollowUp,
  taskMatchesTradeQuery,
  taskIntersectsLookaheadWindow,
  getDateKeyWindowForLookaheadRange,
  formatLookaheadActivitiesReply,
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

test("sanitizeRoutePayload preserves emojis and icons from the user's original text", () => {
  const routed = sanitizeRoutePayload(
    {
      title: "West corridor update",
      description: "West corridor ready for inspection tomorrow.",
      tags: ["progress"],
      requiresFollowUp: false,
    },
    "West corridor ready ✅ for inspection tomorrow. Crane access ⚠️ stays tight.",
    0
  );

  assert.equal(
    routed.description,
    "West corridor ready ✅ for inspection tomorrow. Crane access ⚠️ stays tight."
  );
  assert.match(routed.title, /✅/);
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

test("sanitizeAssistantActionPlan constrains AI action routing output", () => {
  const payload = sanitizeAssistantActionPlan({
    action: "lookahead_trade_query",
    confidence: 1.2,
    reason: "clear weekly trade query",
    tradeQuery: "ALC  ",
    range: "next_week",
    timerLabel: "ignored here",
    todoText: "follow up west wall patch",
    todoDueWindow: "next_week",
    notifyAudience: "management",
    notifyMessage: "crane moved to 10am",
    proposedNotes: "Latest notes",
    deficiency: {
      title: "Lobby tile cracked",
      description: "Replace cracked tile at lobby entry.",
    },
  });
  assert.equal(payload.action, "lookahead_trade_query");
  assert.equal(payload.confidence, 1);
  assert.equal(payload.tradeQuery, "ALC");
  assert.equal(payload.range, "next_week");
  assert.equal(payload.todoText, "follow up west wall patch");
  assert.equal(payload.todoDueWindow, "next_week");
  assert.equal(payload.notifyAudience, "management");
  assert.equal(payload.notifyMessage, "crane moved to 10am");
  assert.equal(payload.proposedNotes, "Latest notes");
  assert.equal(payload.deficiency.title, "Lobby tile cracked");

  const fallback = sanitizeAssistantActionPlan({
    action: "delete_everything",
    confidence: -4,
    range: "forever",
    notifyAudience: "everyone",
    todoDueWindow: "tomorrow",
  });
  assert.equal(fallback.action, "none");
  assert.equal(fallback.confidence, 0);
  assert.equal(fallback.range, "");
  assert.equal(fallback.notifyAudience, "");
  assert.equal(fallback.todoDueWindow, "");
});

test("timer command parsing handles start and stop texts", () => {
  assert.deepEqual(parseStartTimerCommand("start timer for concrete pour"), { label: "concrete pour" });
  assert.deepEqual(parseStartTimerCommand("start timer"), { label: "general task" });
  assert.equal(isStopTimerCommand("stop timer"), true);
  assert.equal(isStopTimerCommand("stop timer now"), true);
  assert.equal(isStopTimerCommand("timer stop"), false);
});

test("project switch guard only allows explicit project-set wording", () => {
  assert.equal(isExplicitProjectSetRequest("project docksteader", "docksteader"), true);
  assert.equal(isExplicitProjectSetRequest("switch project to docksteader", "docksteader"), true);
  assert.equal(isExplicitProjectSetRequest("docksteader project", "docksteader"), true);
  assert.equal(isExplicitProjectSetRequest("SteelCon foreman Gord", "docksteader"), false);
  assert.equal(isExplicitProjectSetRequest("ALC foreman Matheson", "docksteader"), false);
});

test("recent correction search falls back across prior days when no date is given", () => {
  assert.deepEqual(buildRecentCorrectionDateKeys("2026-05-19", "2026-05-21", 7), ["2026-05-19"]);
  assert.deepEqual(buildRecentCorrectionDateKeys(null, "2026-05-21", 4), [
    "2026-05-21",
    "2026-05-20",
    "2026-05-19",
    "2026-05-18",
  ]);
});

test("formatDurationFromMs renders SMS-friendly duration", () => {
  assert.equal(formatDurationFromMs(4 * 60 * 1000), "4m");
  assert.equal(formatDurationFromMs(60 * 60 * 1000), "1h");
  assert.equal(formatDurationFromMs(74 * 60 * 1000), "1h 14m");
});

test("journal auto-save heuristic distinguishes questions from diary updates", () => {
  assert.equal(looksLikeExplicitAiChatRequest("How should I structure my day?"), true);
  assert.equal(looksLikeExplicitAiChatRequest("Today I feel tired and plan to slow down a bit"), false);
  assert.equal(looksLikeExplicitAiChatRequest("What I ate for lunch"), false);
});

test("single-word field prompts stay on the AI request path", () => {
  assert.equal(looksLikeExplicitAiChatRequest("safety"), true);
  assert.equal(looksLikeExplicitAiChatRequest("report"), true);
  assert.equal(looksLikeExplicitAiChatRequest("today"), true);
});

test("explicit labour helpers stay narrow", () => {
  assert.equal(isExplicitLabourEntryText("labour 8.0 framing cleanup"), true);
  assert.equal(isExplicitLabourEntryText("8 hours framing cleanup"), true);
  assert.equal(isExplicitLabourEntryText("Worked on home activities and errands today"), false);
  assert.equal(isExplicitLabourBalanceText("how many hours this week"), true);
  assert.equal(isExplicitLabourBalanceText("today's activities were groceries and cleanup"), false);
});

test("parseLookaheadActivitiesQuery reads trade and week windows", () => {
  assert.deepEqual(parseLookaheadActivitiesQuery("show me the activities for ALC for this week"), {
    tradeQuery: "ALC",
    range: "this_week",
  });
  assert.deepEqual(parseLookaheadActivitiesQuery("ALC activities next week"), {
    tradeQuery: "ALC",
    range: "next_week",
  });
  assert.deepEqual(parseLookaheadActivitiesQuery("show me the activities for this week"), {
    tradeQuery: "",
    range: "this_week",
  });
  assert.equal(parseLookaheadActivitiesQuery("how many hours this week"), null);
});

test("parseTodoDateTimeInput accepts explicit sms todo datetime formats", () => {
  assert.match(parseTodoDateTimeInput("2026-05-30"), /^2026-05-30T/);
  assert.match(parseTodoDateTimeInput("2026-05-30 09:15"), /^2026-05-30T/);
  assert.match(parseTodoDateTimeInput("2026-05-30 9am"), /^2026-05-30T/);
  assert.equal(
    parseTodoDateTimeInput("2026-05-30T13:15:00.000Z"),
    "2026-05-30T13:15:00.000Z"
  );
  assert.equal(parseTodoDateTimeInput("none"), null);
  assert.equal(parseTodoDateTimeInput("next friday"), "");
});

test("pending todo draft advances after none due date and none priority", () => {
  const draft = normalizePendingTodoDraft({
    projectSlug: "home",
    taskText: "follow up supplier",
    sourceText: "xxx follow up supplier",
    dueBy: null,
    dueDateCaptured: true,
    reminderRequested: false,
    secondReminderWanted: false,
    reminders: [],
    priority: null,
    priorityCaptured: true,
    tags: [],
    tagsCaptured: false,
  });
  assert.equal(getNextMissingTodoField(draft), "tags");
});

test("pending todo draft preserves unanswered reminder state", () => {
  const draft = normalizePendingTodoDraft({
    projectSlug: "home",
    taskText: "follow up supplier",
    sourceText: "xxx follow up supplier",
    dueBy: "2026-05-30 17:00",
    dueDateCaptured: true,
    reminderRequested: null,
    secondReminderWanted: null,
    reminders: [],
    priority: null,
    priorityCaptured: false,
    tags: [],
    tagsCaptured: false,
  });
  assert.equal(draft.reminderRequested, null);
  assert.equal(getNextMissingTodoField(draft), "reminderRequested");
});

test("pending todo draft asks for second reminder choice after first reminder is set", () => {
  const draft = normalizePendingTodoDraft({
    projectSlug: "home",
    taskText: "book inspection",
    sourceText: "xxx book inspection",
    dueBy: "2026-05-30 17:00",
    dueDateCaptured: true,
    reminderRequested: true,
    secondReminderWanted: null,
    reminders: ["2026-05-29 09:00"],
    priority: null,
    priorityCaptured: false,
    tags: [],
    tagsCaptured: false,
  });
  assert.equal(getNextMissingTodoField(draft), "secondReminderWanted");
});

test("pending todo draft asks for second reminder datetime after yes", () => {
  const draft = normalizePendingTodoDraft({
    projectSlug: "home",
    taskText: "book inspection",
    sourceText: "xxx book inspection",
    dueBy: "2026-05-30 17:00",
    dueDateCaptured: true,
    reminderRequested: true,
    secondReminderWanted: true,
    reminders: ["2026-05-29 09:00"],
    priority: null,
    priorityCaptured: false,
    tags: [],
    tagsCaptured: false,
  });
  assert.equal(getNextMissingTodoField(draft), "secondReminder");
});

test("pending todo draft reaches tags after second reminder and priority", () => {
  const draft = normalizePendingTodoDraft({
    projectSlug: "home",
    taskText: "book inspection",
    sourceText: "xxx book inspection",
    dueBy: "2026-05-30 17:00",
    dueDateCaptured: true,
    reminderRequested: true,
    secondReminderWanted: true,
    reminders: ["2026-05-29 09:00", "2026-05-30 07:30"],
    priority: "p1",
    priorityCaptured: true,
    tags: [],
    tagsCaptured: false,
  });
  assert.equal(getNextMissingTodoField(draft), "tags");
});

test("pending todo draft preserves stored ISO due date and reminders", () => {
  const draft = normalizePendingTodoDraft({
    projectSlug: "home",
    taskText: "book inspection",
    sourceText: "xxx book inspection",
    dueBy: "2026-05-30T17:00:00.000Z",
    dueDateCaptured: true,
    reminderRequested: true,
    secondReminderWanted: true,
    reminders: ["2026-05-29T09:00:00.000Z", "2026-05-30T07:30:00.000Z"],
    priority: "p1",
    priorityCaptured: true,
    tags: [],
    tagsCaptured: false,
  });
  assert.equal(draft.dueBy, "2026-05-30T17:00:00.000Z");
  assert.deepEqual(draft.reminders, ["2026-05-29T09:00:00.000Z", "2026-05-30T07:30:00.000Z"]);
  assert.equal(getNextMissingTodoField(draft), "tags");
});

test("lookahead helpers filter tasks by week and trade", () => {
  const range = getDateKeyWindowForLookaheadRange("this_week", new Date("2026-05-19T16:00:00Z"));
  assert.deepEqual(range, {
    startKey: "2026-05-18",
    endKey: "2026-05-24",
    label: "this week",
  });

  const alcTask = {
    activity: "Install framing at level 2 west corridor",
    actionBy: "ALC",
    scheduledDateKeys: ["2026-05-19", "2026-05-20"],
    startDate: "2026-05-19",
    finishDate: "2026-05-20",
  };
  const otherTask = {
    activity: "Roof curb layout",
    actionBy: "Roofing",
    scheduledDateKeys: ["2026-05-21"],
    startDate: "2026-05-21",
    finishDate: "2026-05-21",
  };

  assert.equal(taskIntersectsLookaheadWindow(alcTask, range.startKey, range.endKey), true);
  assert.equal(taskMatchesTradeQuery(alcTask, "ALC"), true);
  assert.equal(taskMatchesTradeQuery({ actionBy: "ALC Interiors" }, "ALC"), true);
  assert.equal(taskMatchesTradeQuery(otherTask, "ALC"), false);
});

test("formatLookaheadActivitiesReply summarizes matching weekly tasks", () => {
  const text = formatLookaheadActivitiesReply({
    projectName: "Docksteader",
    tradeQuery: "ALC",
    rangeLabel: "this week",
    startKey: "2026-05-18",
    endKey: "2026-05-24",
    tasks: [
      {
        activity: "Install framing at level 2 west corridor",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-05-19", "2026-05-20"],
        startDate: "2026-05-19",
        finishDate: "2026-05-20",
      },
      {
        activity: "Complete shaft backing",
        actionBy: "ALC",
        scheduledDateKeys: ["2026-05-22"],
        startDate: "2026-05-22",
        finishDate: "2026-05-22",
      },
    ],
  });

  assert.match(text, /Docksteader/);
  assert.match(text, /ALC activities for this week/);
  assert.match(text, /Install framing at level 2 west corridor/);
  assert.match(text, /Complete shaft backing/);
});

test("assistant follow-up helpers recognize short context replies", () => {
  assert.equal(looksLikeAssistantFollowUpAnswer("yes"), true);
  assert.equal(looksLikeAssistantFollowUpAnswer("do that"), true);
  assert.equal(looksLikeAssistantFollowUpAnswer("for the kitchen sink"), true);
  assert.equal(
    looksLikeAssistantFollowUpAnswer("Here is a full separate site update with lots of detail and several unrelated facts."),
    false
  );
  assert.equal(shouldTrackAssistantFollowUp("Should I log this under deficiency?"), true);
  assert.equal(shouldTrackAssistantFollowUp("Saved to the home journal."), false);
});

test("correction follow-up helpers detect correction prompts and affirmative replies", () => {
  assert.equal(looksLikeCorrectionPrompt("Are you sure about the manpower? Do you want to correct it?"), true);
  assert.equal(isAffirmativeCorrectionFollowUp("yes correct it"), true);
  assert.equal(isAffirmativeCorrectionFollowUp("yes"), false);
});

test("narrative correction parser reads common correction phrasing", () => {
  assert.equal(parseNarrativeCorrectionCommand("SteelCon not SteelmCon"), null);
  assert.deepEqual(
    parseNarrativeCorrectionCommand("SteelCon not SteelmCon", { allowShortNotForm: true }),
    {
      target: "SteelmCon",
      replacement: "SteelCon",
      rawText: "SteelCon not SteelmCon",
    }
  );
  assert.deepEqual(parseNarrativeCorrectionCommand("replace Gord with Gordie"), {
    target: "Gord",
    replacement: "Gordie",
    rawText: "replace Gord with Gordie",
  });
  assert.deepEqual(parseNarrativeCorrectionCommand("change SteelmCon to SteelCon"), {
    target: "SteelmCon",
    replacement: "SteelCon",
    rawText: "change SteelmCon to SteelCon",
  });
  assert.equal(
    parseNarrativeCorrectionCommand("I worked on the kitchen sink not the bathroom vanity", {
      allowShortNotForm: true,
    }),
    null
  );
});

test("applyManpowerCorrectionToEntry rewrites the matching manpower count", () => {
  const updated = applyManpowerCorrectionToEntry(
    {
      rawText: "manpower ALC 14 Matheson 6",
      normalizedText: "ALC 14 Matheson 6",
      tags: ["manpower"],
      dailySummarySections: ["dayLog"],
    },
    { trade: "ALC", workers: "17" }
  );

  assert.ok(updated);
  assert.match(updated.rawText, /\bALC 17\b/);
  assert.match(updated.normalizedText, /\bALC 17\b/);
  assert.ok(updated.tags.includes("manpower"));
  assert.ok(updated.dailySummarySections.includes("dayLog"));
});

test("applyNarrativeCorrectionToEntry rewrites the matching text in a saved log entry", () => {
  const updated = applyNarrativeCorrectionToEntry(
    {
      rawText: "SteelmCon erected the big truss.",
      normalizedText: "SteelmCon erected the big truss.",
    },
    {
      target: "SteelmCon",
      replacement: "SteelCon",
    }
  );

  assert.ok(updated);
  assert.equal(updated.rawText, "SteelCon erected the big truss.");
  assert.equal(updated.normalizedText, "SteelCon erected the big truss.");
});

test("safe fallback routing saves narrative text on low-confidence request classifications", () => {
  assert.equal(
    looksLikeNarrativeSaveCandidate("We did lots of activities today and bought a desk for Myles after breakfast."),
    true
  );
  const decision = decideFallbackRouting(
    { intent: "request", confidence: 0.55, reason: "uncertain", source: "ai" },
    "We did lots of activities today and bought a desk for Myles after breakfast.",
    false
  );
  assert.equal(decision.action, "save_log");
  assert.equal(decision.safeFallbackUsed, true);
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

test('parseHomeTodoCommand extracts case-insensitive "xxx" home todos', () => {
  const nextWeek = parseHomeTodoCommand("xxX fix the garage door by next week");
  assert.equal(nextWeek.projectSlug, "home");
  assert.equal(nextWeek.taskText, "fix the garage door");
  assert.equal(nextWeek.dueWindow, "next_week");
  assert.equal(nextWeek.dueLabel, "next week");
  assert.equal(nextWeek.rawText, "fix the garage door by next week");
  assert.match(String(nextWeek.dueByIso || ""), /^\d{4}-\d{2}-\d{2}T/);

  const nextMonth = parseHomeTodoCommand("XXX fix the garage door in the next month");
  assert.equal(nextMonth.projectSlug, "home");
  assert.equal(nextMonth.taskText, "fix the garage door");
  assert.equal(nextMonth.dueWindow, "next_month");
  assert.equal(nextMonth.dueLabel, "next month");
  assert.equal(nextMonth.rawText, "fix the garage door in the next month");
  assert.match(String(nextMonth.dueByIso || ""), /^\d{4}-\d{2}-\d{2}T/);

  assert.deepEqual(
    parseHomeTodoCommand("xxx fix the garage door"),
    {
      projectSlug: "home",
      taskText: "fix the garage door",
      dueWindow: null,
      dueLabel: null,
      dueByIso: null,
      tags: [],
      rawText: "fix the garage door",
    }
  );
});

test("parseTodoReportRequest recognizes todo PDF and Excel export commands", () => {
  assert.deepEqual(parseTodoReportRequest("todo report pdf"), {
    projectSlug: "home",
    format: "pdf",
  });
  assert.deepEqual(parseTodoReportRequest("todo report excel"), {
    projectSlug: "home",
    format: "xlsx",
  });
  assert.equal(parseTodoReportRequest("todo fix the garage door"), null);
});

test("parseTodoListRequest recognizes open and tagged todo list commands", () => {
  assert.deepEqual(parseTodoListRequest("show me all open todo's"), {
    projectSlug: null,
    status: "open",
    priority: "",
    tags: [],
    mineOnly: true,
  });
  assert.deepEqual(parseTodoListRequest('show me the todo "@home"'), {
    projectSlug: null,
    status: "active",
    priority: "",
    tags: ["home"],
    mineOnly: true,
  });
  assert.deepEqual(parseTodoListRequest("show me my todos"), {
    projectSlug: null,
    status: "active",
    priority: "",
    tags: [],
    mineOnly: true,
  });
  assert.deepEqual(parseTodoListRequest("show me my completed p1 todos for docksteader"), {
    projectSlug: "docksteader",
    status: "completed",
    priority: "p1",
    tags: [],
    mineOnly: true,
  });
  assert.deepEqual(parseTodoListRequest("show me my in progress todos for this project", "home"), {
    projectSlug: "home",
    status: "inprogress",
    priority: "",
    tags: [],
    mineOnly: true,
  });
  assert.equal(parseTodoListRequest("todo report pdf"), null);
  assert.equal(parseTodoListRequest("todo fix garage door"), null);
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
