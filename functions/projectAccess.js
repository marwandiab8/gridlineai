const COL_USERS = "smsUsers";
const COL_PROJECTS = "projects";

function normalizeProjectSlug(value) {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return normalized.slice(0, 80);
}

function normalizeProjectName(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 160);
}

function normalizeProjectLocation(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 220);
  return normalized || null;
}

function uniqueProjectSlugs(values) {
  const out = [];
  for (const raw of Array.isArray(values) ? values : []) {
    const slug = normalizeProjectSlug(raw);
    if (slug && !out.includes(slug)) out.push(slug);
  }
  return out;
}

function getUserActiveProjectSlug(userData) {
  const slug = normalizeProjectSlug(userData && userData.activeProjectSlug);
  return slug || null;
}

function getUserProjectSlugs(userData) {
  const slugs = uniqueProjectSlugs(userData && userData.projectSlugs);
  const active = getUserActiveProjectSlug(userData);
  if (active && !slugs.includes(active)) slugs.unshift(active);
  return slugs;
}

function buildUserProjectPatch(userData, projectSlug, options = {}) {
  const slug = normalizeProjectSlug(projectSlug);
  const currentSlugs = getUserProjectSlugs(userData);
  const nextSlugs = slug && !currentSlugs.includes(slug)
    ? [...currentSlugs, slug]
    : currentSlugs;
  const currentActive = getUserActiveProjectSlug(userData);
  const nextActive =
    options.activeProjectSlug === undefined
      ? currentActive || slug || null
      : normalizeProjectSlug(options.activeProjectSlug) || null;

  const patch = {};
  const rawStored = uniqueProjectSlugs(userData && userData.projectSlugs);
  if (JSON.stringify(rawStored) !== JSON.stringify(nextSlugs)) {
    patch.projectSlugs = nextSlugs;
  }
  if ((currentActive || null) !== (nextActive || null)) {
    patch.activeProjectSlug = nextActive;
  }
  return patch;
}

async function getUserProjectAccess(db, phoneE164) {
  const userRef = db.collection(COL_USERS).doc(phoneE164);
  const userSnap = await userRef.get();
  const userData = userSnap.exists ? userSnap.data() || {} : null;
  return {
    exists: userSnap.exists,
    phoneE164,
    userRef,
    userSnap,
    userData,
    activeProjectSlug: getUserActiveProjectSlug(userData),
    projectSlugs: getUserProjectSlugs(userData),
  };
}

async function getProjectRecord(db, projectSlug) {
  const slug = normalizeProjectSlug(projectSlug);
  if (!slug) {
    return {
      exists: false,
      projectSlug: null,
      projectRef: null,
      projectSnap: null,
      projectData: null,
      ownerPhoneE164: null,
    };
  }
  const projectRef = db.collection(COL_PROJECTS).doc(slug);
  const projectSnap = await projectRef.get();
  const projectData = projectSnap.exists ? projectSnap.data() || {} : null;
  const ownerPhoneE164 =
    projectData && typeof projectData.ownerPhoneE164 === "string"
      ? String(projectData.ownerPhoneE164).trim()
      : null;
  return {
    exists: projectSnap.exists,
    projectSlug: slug,
    projectRef,
    projectSnap,
    projectData,
    ownerPhoneE164: ownerPhoneE164 || null,
  };
}

function userCanAccessProject(phoneE164, userData, projectData, projectSlug) {
  const slug = normalizeProjectSlug(projectSlug);
  if (!slug) return true;
  if (!projectData) return false;

  const ownerPhoneE164 =
    typeof projectData.ownerPhoneE164 === "string"
      ? String(projectData.ownerPhoneE164).trim()
      : "";
  if (ownerPhoneE164) {
    return ownerPhoneE164 === String(phoneE164 || "").trim();
  }

  return getUserProjectSlugs(userData).includes(slug);
}

async function getAccessibleProjectForUser(db, phoneE164, projectSlug, userAccessIn) {
  const slug = normalizeProjectSlug(projectSlug);
  const userAccess = userAccessIn || await getUserProjectAccess(db, phoneE164);
  if (!slug) {
    return {
      allowed: true,
      reason: null,
      projectSlug: null,
      projectData: null,
      ownerPhoneE164: null,
      userAccess,
    };
  }

  const projectRecord = await getProjectRecord(db, slug);
  if (!projectRecord.exists) {
    return {
      allowed: false,
      reason: "project_missing",
      ...projectRecord,
      userAccess,
    };
  }

  const allowed = userCanAccessProject(
    phoneE164,
    userAccess.userData,
    projectRecord.projectData,
    slug
  );
  return {
    allowed,
    reason: allowed
      ? null
      : projectRecord.ownerPhoneE164
        ? "project_owned_by_other_user"
        : "project_not_assigned_to_user",
    ...projectRecord,
    userAccess,
  };
}

module.exports = {
  COL_USERS,
  COL_PROJECTS,
  normalizeProjectSlug,
  normalizeProjectName,
  normalizeProjectLocation,
  uniqueProjectSlugs,
  getUserActiveProjectSlug,
  getUserProjectSlugs,
  buildUserProjectPatch,
  getUserProjectAccess,
  getProjectRecord,
  userCanAccessProject,
  getAccessibleProjectForUser,
};
