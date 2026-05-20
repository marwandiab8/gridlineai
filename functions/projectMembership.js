const COL_PROJECT_MEMBERS = "projectMembers";

function normalizeProjectSlug(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized.slice(0, 80);
}

function normalizeMemberEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeMemberPhone(value) {
  return String(value || "").trim();
}

function buildEmailMemberKey(email) {
  const normalized = normalizeMemberEmail(email);
  return normalized ? `email:${normalized}` : "";
}

function buildPhoneMemberKey(phoneE164) {
  const normalized = normalizeMemberPhone(phoneE164);
  return normalized ? `phone:${normalized}` : "";
}

function buildProjectMemberDocId(projectSlug, memberKey) {
  const slug = normalizeProjectSlug(projectSlug);
  const key = String(memberKey || "").trim();
  return slug && key ? `${slug}__${key}` : "";
}

function normalizeProjectRole(value) {
  const role = String(value || "").trim().toLowerCase();
  if (role === "admin" || role === "management" || role === "viewer") return role;
  return "user";
}

function uniqueProjectSlugs(values) {
  const out = [];
  for (const raw of Array.isArray(values) ? values : []) {
    const slug = normalizeProjectSlug(raw);
    if (slug && !out.includes(slug)) out.push(slug);
  }
  return out;
}

function mergeProjectSlugs(...sources) {
  return uniqueProjectSlugs(sources.flat());
}

async function loadProjectMembershipRowsByMemberKey(db, memberKey) {
  const key = String(memberKey || "").trim();
  if (!key) return [];
  const snap = await db
    .collection(COL_PROJECT_MEMBERS)
    .where("memberKey", "==", key)
    .where("active", "==", true)
    .get()
    .catch(() => null);
  if (!snap || snap.empty) return [];
  return snap.docs.map((docSnap) => ({
    id: docSnap.id,
    ref: docSnap.ref,
    ...docSnap.data(),
  }));
}

async function loadProjectMembershipRows(db, { email, phoneE164 } = {}) {
  const keys = [buildEmailMemberKey(email), buildPhoneMemberKey(phoneE164)].filter(Boolean);
  if (!keys.length) return [];
  const chunks = await Promise.all(keys.map((key) => loadProjectMembershipRowsByMemberKey(db, key)));
  const merged = new Map();
  for (const rows of chunks) {
    for (const row of rows) {
      if (!row || !row.id) continue;
      merged.set(row.id, row);
    }
  }
  return Array.from(merged.values());
}

async function loadProjectMembershipSlugs(db, identity) {
  const rows = await loadProjectMembershipRows(db, identity);
  return uniqueProjectSlugs(rows.map((row) => row.projectSlug));
}

async function hasProjectMembership(db, identity, projectSlug) {
  const slug = normalizeProjectSlug(projectSlug);
  if (!slug) return false;
  const slugs = await loadProjectMembershipSlugs(db, identity);
  return slugs.includes(slug);
}

async function syncProjectMemberships(db, FieldValue, input) {
  const {
    projectSlugs,
    role,
    active = true,
    updatedByEmail = null,
    source = "sync",
    displayName = null,
    canApproveNotes = false,
    email = null,
    phoneE164 = null,
  } = input || {};

  const desiredSlugs = uniqueProjectSlugs(projectSlugs);
  const identities = [
    {
      memberKey: buildEmailMemberKey(email),
      memberType: "email",
      memberValue: normalizeMemberEmail(email),
    },
    {
      memberKey: buildPhoneMemberKey(phoneE164),
      memberType: "phone",
      memberValue: normalizeMemberPhone(phoneE164),
    },
  ].filter((item) => item.memberKey && item.memberValue);

  for (const identity of identities) {
    const existingRows = await loadProjectMembershipRowsByMemberKey(db, identity.memberKey);
    const existingBySlug = new Map(
      existingRows
        .map((row) => [normalizeProjectSlug(row.projectSlug), row])
        .filter(([slug]) => Boolean(slug))
    );
    const batch = db.batch();

    for (const slug of desiredSlugs) {
      const docId = buildProjectMemberDocId(slug, identity.memberKey);
      if (!docId) continue;
      const payload = {
        projectSlug: slug,
        memberKey: identity.memberKey,
        memberType: identity.memberType,
        memberValue: identity.memberValue,
        role: normalizeProjectRole(role),
        active: active !== false,
        displayName: String(displayName || "").trim() || null,
        canApproveNotes: canApproveNotes === true,
        source: String(source || "sync").trim() || "sync",
        ...(identity.memberType === "email" ? { email: identity.memberValue } : {}),
        ...(identity.memberType === "phone" ? { phoneE164: identity.memberValue } : {}),
        updatedAt: FieldValue.serverTimestamp(),
        ...(updatedByEmail ? { updatedByEmail } : {}),
      };
      if (!existingBySlug.has(slug)) {
        payload.createdAt = FieldValue.serverTimestamp();
      }
      batch.set(
        db.collection(COL_PROJECT_MEMBERS).doc(docId),
        payload,
        { merge: true }
      );
      existingBySlug.delete(slug);
    }

    for (const [, row] of existingBySlug.entries()) {
      batch.delete(db.collection(COL_PROJECT_MEMBERS).doc(row.id));
    }

    await batch.commit();
  }
}

module.exports = {
  COL_PROJECT_MEMBERS,
  normalizeProjectSlug,
  normalizeMemberEmail,
  normalizeMemberPhone,
  buildEmailMemberKey,
  buildPhoneMemberKey,
  buildProjectMemberDocId,
  normalizeProjectRole,
  uniqueProjectSlugs,
  mergeProjectSlugs,
  loadProjectMembershipRowsByMemberKey,
  loadProjectMembershipRows,
  loadProjectMembershipSlugs,
  hasProjectMembership,
  syncProjectMemberships,
};
