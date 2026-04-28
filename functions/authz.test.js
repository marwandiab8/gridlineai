const { test } = require("node:test");
const assert = require("node:assert/strict");
const {
  getAppAccess,
  getOperatorAccess,
  canAccessProject,
  canApproveProjectNoteRequests,
} = require("./authz");

function makeDb({ operatorDocs = {}, memberDocs = {} } = {}) {
  return {
    collection(name) {
      assert.ok(name === "adminOperators" || name === "appMembers");
      const source = name === "adminOperators" ? operatorDocs : memberDocs;
      return {
        doc(id) {
          return {
            async get() {
              if (Object.prototype.hasOwnProperty.call(source, id)) {
                return {
                  exists: true,
                  data: () => source[id],
                  ref: { id },
                };
              }
              return {
                exists: false,
                data: () => null,
                ref: { id },
              };
            },
          };
        },
      };
    },
  };
}

test("getOperatorAccess accepts approved operator doc", async () => {
  const db = makeDb({ operatorDocs: { "admin@example.com": { active: true, role: "admin" } } });
  const access = await getOperatorAccess(db, {
    auth: {
      uid: "uid-1",
      token: {
        email: "Admin@Example.com",
        email_verified: true,
      },
    },
  });

  assert.equal(access.email, "admin@example.com");
  assert.equal(access.via, "firestore-allowlist");
});

test("getOperatorAccess rejects inactive operator doc", async () => {
  const db = makeDb({ operatorDocs: { "admin@example.com": { active: false } } });

  await assert.rejects(
    () =>
      getOperatorAccess(db, {
        auth: {
          uid: "uid-1",
          token: {
            email: "admin@example.com",
            email_verified: true,
          },
        },
      }),
    /not approved for app access/
  );
});

test("getOperatorAccess accepts admin custom claim", async () => {
  const db = makeDb();
  const access = await getOperatorAccess(db, {
    auth: {
      uid: "uid-1",
      token: {
        email: "admin@example.com",
        email_verified: true,
        admin: true,
      },
    },
  });

  assert.equal(access.email, "admin@example.com");
  assert.equal(access.via, "custom-claim");
});

test("getAppAccess accepts active app member with management role", async () => {
  const db = makeDb({
    memberDocs: {
      "manager@example.com": {
        active: true,
        role: "management",
        projectSlugs: ["Alpha-Tower", "beta-west"],
        canApproveNotes: true,
      },
    },
  });
  const access = await getAppAccess(db, {
    auth: {
      uid: "uid-2",
      token: {
        email: "manager@example.com",
        email_verified: true,
      },
    },
  });

  assert.equal(access.role, "management");
  assert.deepEqual(access.projectSlugs, ["alpha-tower", "beta-west"]);
  assert.equal(canAccessProject(access, "beta-west"), true);
  assert.equal(canApproveProjectNoteRequests(access), true);
});

test("getAppAccess rejects unknown members", async () => {
  const db = makeDb();
  await assert.rejects(
    () =>
      getAppAccess(db, {
        auth: {
          uid: "uid-3",
          token: {
            email: "user@example.com",
            email_verified: true,
          },
        },
      }),
    /not approved for app access/
  );
});

test("getOperatorAccess rejects management when minimum is default admin", async () => {
  const db = makeDb({
    memberDocs: {
      "manager@example.com": {
        active: true,
        role: "management",
        projectSlugs: ["site-a"],
      },
    },
  });
  await assert.rejects(
    () =>
      getOperatorAccess(db, {
        auth: {
          uid: "uid-m",
          token: { email: "manager@example.com", email_verified: true },
        },
      }),
    /operator access/
  );
});

test("getOperatorAccess accepts management when minimumRole is management", async () => {
  const db = makeDb({
    memberDocs: {
      "manager@example.com": {
        active: true,
        role: "management",
        projectSlugs: ["site-a"],
      },
    },
  });
  const access = await getOperatorAccess(
    db,
    {
      auth: {
        uid: "uid-m",
        token: { email: "manager@example.com", email_verified: true },
      },
    },
    { minimumRole: "management" }
  );
  assert.equal(access.role, "management");
});
