const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  addDashboardIssueNote,
  attachDashboardIssuePhoto,
  createDashboardIssue,
  deleteDashboardIssue,
  normalizeDueDateInput,
  sanitizeDashboardIssueFields,
  updateDashboardIssue,
} = require("./issueRepository");

function makeFieldValue() {
  return {
    serverTimestamp() {
      return { __type: "serverTimestamp" };
    },
  };
}

function makeDb(seed = {}) {
  let nextId = 1;
  const store = new Map(Object.entries(seed).map(([path, value]) => [path, structuredClone(value)]));

  function clone(value) {
    return value == null ? value : structuredClone(value);
  }

  function mergeInto(target, source) {
    const out = { ...(target || {}) };
    for (const [key, value] of Object.entries(source || {})) {
      out[key] = value;
    }
    return out;
  }

  return {
    store,
    collection(name) {
      const filters = [];
      const makeQuery = () => ({
        where(field, op, value) {
          filters.push({ field, op, value });
          return makeQuery();
        },
        async get() {
          const docs = [];
          for (const [path, value] of store.entries()) {
            if (!path.startsWith(`${name}/`)) continue;
            const docId = path.slice(name.length + 1);
            const matches = filters.every(({ field, op, value: expected }) => {
              if (op !== "==") throw new Error(`Unsupported test operator: ${op}`);
              return (value || {})[field] === expected;
            });
            if (!matches) continue;
            docs.push({
              id: docId,
              ref: {
                async delete() {
                  store.delete(path);
                },
              },
              data: () => clone(value),
            });
          }
          return {
            docs,
            size: docs.length,
          };
        },
      });
      return {
        doc(id) {
          const docId = id || `${name}-${nextId++}`;
          const path = `${name}/${docId}`;
          return {
            id: docId,
            async get() {
              const current = store.get(path);
              return {
                id: docId,
                exists: current !== undefined,
                data: () => clone(current),
              };
            },
            async set(value, options = {}) {
              const existing = store.get(path);
              const next = options.merge ? mergeInto(existing, clone(value)) : clone(value);
              store.set(path, next);
            },
            async delete() {
              store.delete(path);
            },
          };
        },
        async add(value) {
          const ref = this.doc();
          await ref.set(value);
          return ref;
        },
        where(field, op, value) {
          return makeQuery().where(field, op, value);
        },
      };
    },
  };
}

test("normalizeDueDateInput accepts blank or yyyy-mm-dd", () => {
  assert.equal(normalizeDueDateInput("").timestamp, null);
  assert.throws(() => normalizeDueDateInput("04/18/2026"), /YYYY-MM-DD/);
  assert.equal(normalizeDueDateInput("2026-04-18").dateKey, "2026-04-18");
});

test("sanitizeDashboardIssueFields validates type, status, and priority", () => {
  assert.throws(
    () =>
      sanitizeDashboardIssueFields({
        type: "wat",
        status: "Open",
        priority: "Medium",
      }),
    /type must be one of/
  );
  assert.throws(
    () =>
      sanitizeDashboardIssueFields({
        type: "general",
        status: "Broken",
        priority: "Medium",
      }),
    /status is invalid/
  );
});

test("createDashboardIssue creates a typed issue with server-side history", async () => {
  const db = makeDb({
    "projects/proj-1": { name: "Project One" },
  });
  const FieldValue = makeFieldValue();

  const created = await createDashboardIssue(db, FieldValue, {
    operator: { uid: "uid-1", email: "ops@example.com" },
    type: "deficiency",
    projectId: "proj-1",
    title: "Patch drywall bulkhead",
    description: "Drywall bulkhead patch is incomplete in unit 304.",
    location: "Unit 304",
    area: "Kitchen",
    trade: "Drywall",
    reference: "A6.2",
    requestedAction: "Patch, sand, and repaint.",
    status: "Open",
    priority: "High",
    assignedTo: "Drywall Foreman",
    dueDate: "2026-04-18",
    tags: ["punch", "drywall"],
  });

  const stored = db.store.get(`${created.issueCollection}/${created.issueId}`);
  assert.equal(created.issueCollection, "deficiencyIssues");
  assert.equal(stored.projectName, "Project One");
  assert.equal(stored.reportedByName, "ops@example.com");
  assert.equal(stored.history.length, 1);
  assert.match(stored.history[0].note, /created from dashboard/i);
});

test("updateDashboardIssue appends change history and preserves closedAt on reopen", async () => {
  const db = makeDb({
    "generalIssues/issue-1": {
      issueId: "issue-1",
      issueType: "general",
      status: "Closed",
      priority: "Medium",
      assignedTo: "",
      dueDate: null,
      location: "",
      area: "",
      trade: "",
      reference: "",
      requestedAction: "",
      description: "Old description",
      closedAt: { persisted: true },
      history: [],
    },
  });
  const FieldValue = makeFieldValue();

  await updateDashboardIssue(db, FieldValue, {
    operator: { uid: "uid-2", email: "pm@example.com" },
    issueCollection: "generalIssues",
    issueId: "issue-1",
    status: "Open",
    priority: "Critical",
    assignedTo: "PM",
    dueDate: "2026-04-19",
    location: "Level 2",
    area: "Lobby",
    trade: "GC",
    reference: "SK-1",
    requestedAction: "Review and coordinate",
    description: "Updated description",
  });

  const stored = db.store.get("generalIssues/issue-1");
  assert.equal(stored.priority, "Critical");
  assert.equal(stored.assignedTo, "PM");
  assert.equal(stored.closedAt.persisted, true);
  assert.ok(Array.isArray(stored.history));
  assert.ok(stored.history.some((entry) => entry.field === "priority"));
  assert.ok(stored.history.some((entry) => entry.field === "dueDate"));
});

test("addDashboardIssueNote appends a note entry", async () => {
  const db = makeDb({
    "safetyIssues/issue-2": {
      issueId: "issue-2",
      history: [],
    },
  });
  const FieldValue = makeFieldValue();

  await addDashboardIssueNote(db, FieldValue, {
    operator: { uid: "uid-3", email: "super@example.com" },
    issueCollection: "safetyIssues",
    issueId: "issue-2",
    note: "Barricade was reset before shift change.",
  });

  const stored = db.store.get("safetyIssues/issue-2");
  assert.equal(stored.history.length, 1);
  assert.equal(stored.history[0].action, "note_added");
  assert.match(stored.history[0].note, /Barricade was reset/);
});

test("attachDashboardIssuePhoto appends a photo record and history entry", async () => {
  const db = makeDb({
    "delayIssues/issue-3": {
      issueId: "issue-3",
      photos: [],
      history: [],
    },
  });
  const FieldValue = makeFieldValue();

  await attachDashboardIssuePhoto(db, FieldValue, {
    operator: { uid: "uid-4", email: "field@example.com" },
    issueCollection: "delayIssues",
    issueId: "issue-3",
    photo: {
      storagePath: "projects/proj-1/media/issues/issue-3/photo.jpg",
      downloadURL: "https://example.com/photo.jpg",
      fileName: "photo.jpg",
      mimeType: "image/jpeg",
    },
  });

  const stored = db.store.get("delayIssues/issue-3");
  assert.equal(stored.photos.length, 1);
  assert.equal(stored.photos[0].uploadedBy, "field@example.com");
  assert.equal(stored.history.length, 1);
  assert.equal(stored.history[0].action, "photo_added");
});

test("deleteDashboardIssue removes the typed issue and linked legacy issue logs", async () => {
  const db = makeDb({
    "generalIssues/issue-4": {
      issueId: "issue-4",
      issueType: "general",
      title: "Temporary issue",
    },
    "issueLogs/log-1": {
      canonicalIssueId: "issue-4",
      issueCollection: "generalIssues",
      type: "issue",
    },
    "issueLogs/log-2": {
      canonicalIssueId: "issue-4",
      issueCollection: "generalIssues",
      type: "issue",
    },
    "issueLogs/log-3": {
      canonicalIssueId: "issue-4",
      issueCollection: "delayIssues",
      type: "issue",
    },
  });

  const result = await deleteDashboardIssue(db, {
    issueCollection: "generalIssues",
    issueId: "issue-4",
  });

  assert.equal(result.issueId, "issue-4");
  assert.equal(result.issueCollection, "generalIssues");
  assert.equal(result.deletedLegacyCount, 2);
  assert.equal(db.store.has("generalIssues/issue-4"), false);
  assert.equal(db.store.has("issueLogs/log-1"), false);
  assert.equal(db.store.has("issueLogs/log-2"), false);
  assert.equal(db.store.has("issueLogs/log-3"), true);
});
