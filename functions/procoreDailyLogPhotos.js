// The Procore daily log's photos, for a project's Daily Site Log (Docksteader).
//
// The procore-daily-notes job sends the day's photo list with the completed log (procoreDailyLog.js): each photo's
// time, who took it, its star, and Procore's signed file links (storage.procore.com, readable without a sign-in).
// storeProcoreDailyLog queues the list in procorePhotoQueue/{projectSlug}__{dateKey}; processProcorePhotoQueue picks
// a few to show large (the starred ones, else some spread through the day) and up to 24 small ones spread through the
// day, keeps its own copies in Storage under procoreDailyLogs/{projectSlug}/{dateKey}/ (large ones at good
// resolution, small ones as low-resolution thumbnails), records them as the log's photoSet, and deletes the queue
// entry so the signed links aren't kept.
const crypto = require("crypto");

const COL_PROCORE_PHOTO_QUEUE = "procorePhotoQueue";
const PROCORE_FILE_HOST = "storage.procore.com";
const MAX_SOURCES = 300;
const LARGE_COUNT = 4;
const SMALL_COUNT = 24;
const LARGE_PX = 1600;
const SMALL_PX = 360;

const clean = (value, max = 200) => String(value == null ? "" : value).replace(/\s+/g, " ").trim().slice(0, max);

/** A Procore file link, or "" for anything else (the server only ever downloads from Procore's file storage). */
function procoreFileUrl(value) {
  try {
    const url = new URL(String(value || ""));
    return url.protocol === "https:" && url.hostname === PROCORE_FILE_HOST ? url.toString() : "";
  } catch (_) {
    return "";
  }
}

function isoOrEmpty(value) {
  const ms = Date.parse(String(value || ""));
  return Number.isFinite(ms) ? new Date(ms).toISOString() : "";
}

/** The photo list as the export sends it (Procore's image fields), checked and trimmed. */
function normalizeProcorePhotoSources(list) {
  return (Array.isArray(list) ? list : [])
    .slice(0, MAX_SOURCES)
    .filter((p) => p && typeof p === "object")
    .map((p) => ({
      id: clean(p.id, 40).replace(/[^\w-]/g, ""),
      takenAt: isoOrEmpty(p.taken_at || p.takenAt || p.created_at),
      uploader: clean(p.uploader, 80),
      category: clean(p.category, 80),
      starred: p.starred === true,
      description: clean(p.description, 300),
      thumbUrl: procoreFileUrl(p.thumbnail_url || p.thumbUrl),
      fullUrl: procoreFileUrl(p.url || p.fullUrl),
    }))
    .filter((p) => p.id && (p.thumbUrl || p.fullUrl));
}

/** Changes when photos are added or removed or a star changes: then the picks are made again. */
function procorePhotoSourceHash(sources) {
  const key = sources.map((p) => `${p.id}:${p.starred ? 1 : 0}`).sort().join(",");
  return crypto.createHash("sha256").update(key).digest("hex");
}

/** `count` items spread evenly through `list` (all of them when there aren't more). */
function spread(list, count) {
  if (list.length <= count) return list.slice();
  return Array.from({ length: count }, (_, i) => list[Math.floor(((i + 0.5) * list.length) / count)]);
}

/**
 * The photos to show: up to 4 large (the starred ones, else spread through the day) and up to 24 small ones spread
 * through the rest of the day, each list in the order they were taken.
 */
function selectProcorePhotos(sources, { large = LARGE_COUNT, small = SMALL_COUNT } = {}) {
  const byTime = sources
    .slice()
    .sort((a, b) => (a.takenAt || "").localeCompare(b.takenAt || "") || a.id.localeCompare(b.id));
  const starred = byTime.filter((p) => p.starred);
  const big = spread(starred.length ? starred : byTime, large);
  const bigIds = new Set(big.map((p) => p.id));
  return { large: big, thumbs: spread(byTime.filter((p) => !bigIds.has(p.id)), small), starred: starred.length > 0 };
}

/**
 * Downloads the picked photos, keeps them in Storage (large ones at good resolution, small ones as thumbnails),
 * records them as the log's photoSet and deletes the queue entry. A photo that can't be fetched is skipped.
 */
async function processProcorePhotoQueue({ db, FieldValue, bucket, logger, ref, queue, fetchImpl = fetch, sharp = require("sharp") }) {
  const { projectSlug, dateKey, sourceHash } = queue || {};
  const sources = normalizeProcorePhotoSources(queue && queue.sources);
  const logRef = db.collection("procoreDailyLogs").doc(`${projectSlug}__${dateKey}`);
  const folder = `procoreDailyLogs/${projectSlug}/${dateKey}`;
  const pick = selectProcorePhotos(sources);
  let failed = 0;

  async function keep(photo, kind) {
    const url = kind === "large" ? photo.fullUrl || photo.thumbUrl : photo.thumbUrl || photo.fullUrl;
    const res = await fetchImpl(url, { signal: AbortSignal.timeout(30000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const px = kind === "large" ? LARGE_PX : SMALL_PX;
    const jpeg = await sharp(Buffer.from(await res.arrayBuffer()))
      .rotate()
      .resize({ width: px, height: px, fit: "inside", withoutEnlargement: true })
      .jpeg({ quality: kind === "large" ? 80 : 70, mozjpeg: true })
      .toBuffer();
    const storagePath = `${folder}/${kind}-${photo.id}.jpg`;
    await bucket.file(storagePath).save(jpeg, { contentType: "image/jpeg", resumable: false });
    return { id: photo.id, storagePath, takenAt: photo.takenAt, uploader: photo.uploader, starred: photo.starred };
  }

  async function keepAll(list, kind) {
    const kept = [];
    for (const photo of list) {
      try {
        kept.push(await keep(photo, kind));
      } catch (err) {
        failed += 1;
        logger.warn("procoreDailyLogPhotos: photo skipped", { projectSlug, dateKey, id: photo.id, kind, message: err.message });
      }
    }
    return kept;
  }

  try {
    const large = await keepAll(pick.large, "large");
    const thumbs = await keepAll(pick.thumbs, "small");
    // Photos an earlier pick kept that this one doesn't.
    const keepPaths = new Set([...large, ...thumbs].map((p) => p.storagePath));
    const [files] = await bucket.getFiles({ prefix: `${folder}/` });
    await Promise.all(files.filter((f) => !keepPaths.has(f.name)).map((f) => f.delete().catch(() => {})));
    const status = large.length || thumbs.length || !sources.length ? "done" : "failed";
    await logRef.set(
      { photoSet: { status, sourceHash: sourceHash || procorePhotoSourceHash(sources), total: sources.length, starred: pick.starred, large, thumbs, failed, processedAt: FieldValue.serverTimestamp() } },
      { merge: true }
    );
    logger.info("procoreDailyLogPhotos: kept", { projectSlug, dateKey, total: sources.length, large: large.length, thumbs: thumbs.length, failed });
  } catch (err) {
    logger.error("procoreDailyLogPhotos: failed", { projectSlug, dateKey, message: err.message });
    await logRef.set({ photoSet: { status: "failed", error: String(err.message || err).slice(0, 300), processedAt: FieldValue.serverTimestamp() } }, { merge: true }).catch(() => {});
  } finally {
    await ref.delete().catch(() => {}); // the signed links aren't kept
  }
}

/** The day's kept photos for the site log, or null when there are none (yet). */
async function loadProcorePhotoSet(db, projectSlug, dateKey) {
  if (!projectSlug || !/^\d{4}-\d{2}-\d{2}$/.test(String(dateKey || ""))) return null;
  const snap = await db.collection("procoreDailyLogs").doc(`${projectSlug}__${dateKey}`).get();
  const set = snap.exists ? snap.get("photoSet") : null;
  if (!set || set.status !== "done") return null;
  const list = (value) => (Array.isArray(value) ? value.filter((p) => p && p.storagePath) : []);
  const large = list(set.large);
  const thumbs = list(set.thumbs);
  return large.length || thumbs.length ? { total: Number(set.total) || large.length + thumbs.length, starred: set.starred === true, large, thumbs } : null;
}

/** "4:34 PM" (Toronto time). */
function photoTimeLabel(iso) {
  const ms = Date.parse(String(iso || ""));
  if (!Number.isFinite(ms)) return "";
  return new Intl.DateTimeFormat("en-US", { timeZone: "America/Toronto", hour: "numeric", minute: "2-digit" }).format(new Date(ms));
}

/** "4:34 PM · Dhruv Patel" under a large photo. */
function procorePhotoCaption(photo) {
  return [photoTimeLabel(photo.takenAt), photo.uploader].filter(Boolean).join(" · ");
}

/** "33 photos in the Procore daily log: 4 shown large and 24 more small, picked through the day." */
function describeProcorePhotoSet(set) {
  const shown = set.large.length + set.thumbs.length;
  const large = `${set.large.length}${set.starred ? " starred" : ""} shown large`;
  const parts = [set.large.length ? large : "", set.thumbs.length ? `${set.thumbs.length} ${set.large.length ? "more " : ""}small` : ""].filter(Boolean);
  const picked = set.total > shown ? ", picked through the day" : "";
  return `${set.total} ${set.total === 1 ? "photo" : "photos"} in the Procore daily log: ${parts.join(" and ")}${picked}.`;
}

module.exports = {
  COL_PROCORE_PHOTO_QUEUE,
  normalizeProcorePhotoSources,
  procorePhotoSourceHash,
  selectProcorePhotos,
  processProcorePhotoQueue,
  loadProcorePhotoSet,
  photoTimeLabel,
  procorePhotoCaption,
  describeProcorePhotoSet,
};
