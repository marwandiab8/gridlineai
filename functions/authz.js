const { HttpsError } = require("firebase-functions/v2/https");

const COL_ADMIN_OPERATORS = "adminOperators";
const COL_APP_MEMBERS = "appMembers";
const COL_LABOURERS = "labourers";
const ROLE_ORDER = ["viewer", "user", "management", "admin"];

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeRole(value) {
  const role = String(value || "").trim().toLowerCase();
  if (role === "admin" || role === "management" || role === "viewer") return role;
  return "user";
}

function normalizeApprovedPhone(value) {
  return String(value || "").trim();
}

function roleRank(role) {
  return ROLE_ORDER.indexOf(normalizeRole(role));
}

function roleAtLeast(role, minimumRole) {
  return roleRank(role) >= roleRank(minimumRole);
}

function hasAdminClaim(auth) {
  if (!auth || !auth.token) return false;
  if (auth.token.admin === true) return true;
  if (auth.token.operator === true) return true;
  return String(auth.token.role || "").trim().toLowerCase() === "admin";
}

function assertAuthenticated(request) {
  if (!request || !request.auth || !request.auth.uid) {
    throw new HttpsError("unauthenticated", "Sign in required.");
  }
}

async function getOperatorDoc(db, email) {
  const operatorRef = db.collection(COL_ADMIN_OPERATORS).doc(email);
  const operatorSnap = await operatorRef.get();
  const operatorData = operatorSnap.exists ? operatorSnap.data() || {} : null;
  return {
    operatorRef,
    operatorSnap,
    operatorData,
    active: !!(operatorData && operatorData.active === true),
  };
}

async function getAppMemberDoc(db, email) {
  const memberRef = db.collection(COL_APP_MEMBERS).doc(email);
  const memberSnap = await memberRef.get();
  const memberData = memberSnap.exists ? memberSnap.data() || {} : null;
  return {
    memberRef,
    memberSnap,
    memberData,
    active: !!(memberData && memberData.active === true),
  };
}

async function findActiveAppMemberByApprovedPhone(db, phoneE164) {
  const normalizedPhone = normalizeApprovedPhone(phoneE164);
  if (!normalizedPhone) return null;
  const snap = await db
    .collection(COL_APP_MEMBERS)
    .where("approvedPhoneE164", "==", normalizedPhone)
    .where("active", "==", true)
    .limit(1)
    .get();
  if (snap.empty) return null;
  const docSnap = snap.docs[0];
  const memberData = docSnap.data() || {};
  const role = normalizeRole(memberData.role);
  return {
    email: normalizeEmail(docSnap.id),
    memberDocId: docSnap.id,
    memberData,
    role,
    projectSlugs: normalizeProjectSlugs(memberData.projectSlugs),
    allProjects: memberData.allProjects === true || role === "admin",
    canApproveNotes: role === "admin" || memberData.canApproveNotes === true,
    approvedPhoneE164: normalizedPhone,
    via: "approved-phone",
  };
}

async function findActiveLabourerByPhone(db, phoneE164) {
  const normalizedPhone = normalizeApprovedPhone(phoneE164);
  if (!normalizedPhone) return null;
  const snap = await db.collection(COL_LABOURERS).doc(normalizedPhone).get();
  if (!snap.exists) return null;
  const labourerData = snap.data() || {};
  if (labourerData.active === false) return null;
  return {
    phoneE164: normalizedPhone,
    labourerDocId: snap.id,
    labourerData,
    displayName: String(labourerData.name || labourerData.displayName || "").trim(),
    projectSlugs: normalizeProjectSlugs(labourerData.projectSlugs),
    active: labourerData.active !== false,
    via: "labourer-phone",
  };
}

function normalizeProjectSlugs(values) {
  const out = [];
  for (const raw of Array.isArray(values) ? values : []) {
    const slug = String(raw || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 80);
    if (slug && !out.includes(slug)) out.push(slug);
  }
  return out;
}

async function getAppAccess(db, request, options = {}) {
  assertAuthenticated(request);

  const auth = request.auth;
  const email = normalizeEmail(auth.token && auth.token.email);
  const emailVerified = auth.token ? auth.token.email_verified !== false : false;

  if (!email || !emailVerified) {
    throw new HttpsError(
      "permission-denied",
        "An approved operator account with a verified email is required."
      );
  }

  if (hasAdminClaim(auth)) {
    return {
      auth,
      email,
      memberDocId: email,
      memberData: null,
      role: "admin",
      projectSlugs: [],
      allProjects: true,
      canApproveNotes: true,
      via: "custom-claim",
    };
  }

  const memberRecord = await getAppMemberDoc(db, email);
  const memberData = memberRecord.memberData;
  const memberActive = memberRecord.active;

  if (memberRecord.memberSnap.exists && memberActive) {
    const role = normalizeRole(memberData.role);
    return {
      auth,
      email,
      memberDocId: email,
      memberRef: memberRecord.memberRef,
      memberData,
      role,
      projectSlugs: normalizeProjectSlugs(memberData.projectSlugs),
      allProjects: memberData.allProjects === true || role === "admin",
      canApproveNotes: role === "admin" || memberData.canApproveNotes === true,
      via: "app-member",
    };
  }

  if (options.allowLegacyAdmin !== false) {
    const operatorRecord = await getOperatorDoc(db, email);
    if (operatorRecord.operatorSnap.exists && operatorRecord.active) {
      return {
        auth,
        email,
        operatorDocId: email,
        operatorRef: operatorRecord.operatorRef,
        operatorData: operatorRecord.operatorData,
        memberDocId: email,
        memberData: null,
        role: "admin",
        projectSlugs: [],
        allProjects: true,
        canApproveNotes: true,
        via: "firestore-allowlist",
      };
    }
  }

  if (memberRecord.memberSnap.exists && !memberActive) {
    throw new HttpsError(
      "permission-denied",
      "This account is inactive. Contact an administrator."
    );
  }

  if (options.requireMember === false) {
    return {
      auth,
      email,
      memberDocId: email,
      memberData: null,
      role: "viewer",
      projectSlugs: [],
      allProjects: false,
      canApproveNotes: false,
      via: "none",
    };
  }

  throw new HttpsError(
    "permission-denied",
    "This account is not approved for app access."
  );
}

/**
 * Elevated dashboard access. Default minimum is admin (member management, lookahead parse, etc.).
 * Issue-board callables pass `{ minimumRole: "management" }` so project-scoped managers can mutate issues.
 */
async function getOperatorAccess(db, request, options) {
  const access = await getAppAccess(db, request);
  const minimumRole =
    options &&
    typeof options === "object" &&
    options.minimumRole != null &&
    String(options.minimumRole).trim() !== ""
      ? normalizeRole(String(options.minimumRole).trim())
      : "admin";
  if (!roleAtLeast(access.role, minimumRole)) {
    throw new HttpsError(
      "permission-denied",
      minimumRole === "management"
        ? "This account is not approved for issue management access."
        : "This account is not approved for operator access."
    );
  }
  return access;
}

function canAccessProject(access, projectSlug) {
  const slug = String(projectSlug || "").trim().toLowerCase();
  if (!slug) return roleAtLeast(access && access.role, "management");
  if (!access) return false;
  if (roleAtLeast(access.role, "admin")) return true;
  if (access.allProjects === true) return true;
  const slugs = normalizeProjectSlugs(access.projectSlugs);
  return slugs.includes(slug);
}

function canApproveProjectNoteRequests(access) {
  if (!access) return false;
  if (roleAtLeast(access.role, "admin")) return true;
  return normalizeRole(access.role) === "management" && access.canApproveNotes === true;
}

module.exports = {
  COL_ADMIN_OPERATORS,
  COL_APP_MEMBERS,
  COL_LABOURERS,
  normalizeEmail,
  normalizeRole,
  normalizeApprovedPhone,
  roleAtLeast,
  hasAdminClaim,
  assertAuthenticated,
  getAppAccess,
  getOperatorAccess,
  findActiveAppMemberByApprovedPhone,
  findActiveLabourerByPhone,
  canAccessProject,
  canApproveProjectNoteRequests,
};
