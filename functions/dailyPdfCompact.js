/**
 * PDF-only compaction: dedupe narratives, trim redundant text, and prepare photo priority.
 * Does not change upstream AI or log pipelines — only what the renderer shows.
 */

"use strict";

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^a-z0-9\s]/gi, "")
    .trim();
}

function paragraphContainedInHaystack(para, hayNorm) {
  const p = norm(para);
  if (p.length < 20) return false;
  const slice = p.slice(0, Math.min(120, p.length));
  return hayNorm.includes(slice);
}

function buildWorkHaystack(merged) {
  const parts = [merged.execSummary, merged.workNarrative];
  const secs = merged.workSectionsAi || [];
  for (const s of secs) {
    for (const it of s.items || []) parts.push(it);
  }
  return norm(parts.filter(Boolean).join("\n"));
}

/**
 * Drop issue paragraphs already stated in work / executive content.
 */
function filterIssuesText(issuesText, merged) {
  const workHay = buildWorkHaystack(merged);
  if (!workHay) return issuesText;
  const paras = String(issuesText || "")
    .split(/\n\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
  if (paras.length <= 1) return issuesText;
  const kept = paras.filter((p) => !paragraphContainedInHaystack(p, workHay));
  return kept.length ? kept.join("\n\n") : issuesText;
}

function shouldOmitManpowerNarrative(merged, det) {
  const rows = det.manpowerRows || [];
  if (rows.length < 2) return false;
  const nar = String(merged.manpowerNarrative || "").trim();
  if (!nar || nar === "—") return true;
  const tableJoint = norm(rows.map((r) => r.map((c) => String(c ?? "")).join(" ")).join(" "));
  const words = nar.toLowerCase().match(/[a-z0-9]{4,}/g) || [];
  if (words.length < 5) return false;
  let hit = 0;
  for (const w of words) {
    if (tableJoint.includes(w)) hit++;
  }
  return hit / words.length >= 0.72;
}

/** Single-line roll-call style: "Manpower ALC: 17 Matheson: 7 …" — redundant once the table exists. */
function isRollCallStyleManpowerNarrative(nar) {
  const s = String(nar || "").trim();
  if (!s) return false;
  if (/^manpower\b/i.test(s)) return true;
  const colonHits = (s.match(/:\s*\d+/g) || []).length;
  if (colonHits >= 2) return true;
  return false;
}

function shouldOmitManpowerNarrativeV2(merged, det) {
  const rows = det.manpowerRows || [];
  const nar = String(merged.manpowerNarrative || "").trim();
  if (!nar || nar === "—") return true;
  if (rows.length < 1) return false;
  if (shouldOmitManpowerNarrative(merged, det)) return true;
  if (rows.length >= 1 && isRollCallStyleManpowerNarrative(nar)) return true;
  return false;
}

/**
 * Work-activity lines mis-bucketed as manpower (pour, cleaning, etc.) — omit from Manpower Summary PDF.
 */
function isWorkLikeManpowerLeak(text) {
  const t = String(text || "");
  const workHint =
    /\b(poured|pour|placement|cubic\s*m|m³|\bm3\b|cleaning|worked\s+on|foundation|slab|offload|delivered|placed|rmc)\b/i.test(
      t
    );
  const mpHint =
    /\b(manpower|foreman|headcount|roll\s*call|workers?\s*[:=]|\d+\s*(?:men|workers?|heads?|crew|pax|bodies))\b/i.test(
      t
    ) || /^manpower\b/i.test(t);
  return workHint && !mpHint;
}

function filterManpowerChunksForPdf(chunks) {
  return (chunks || []).filter((ch) => ch && !isWorkLikeManpowerLeak(ch.text));
}

/** Strip SMS/source correction phrasing from final PDF copy. */
function stripSourceLogArtifacts(s) {
  let t = String(s || "");
  t = t.replace(/\bcorrected\s+location\s*:?\s*([^.]+)\.?\s*/gi, (_, loc) => {
    const L = String(loc || "").trim();
    return L.length > 2 ? `Location noted: ${L}. ` : "";
  });
  t = t.replace(/\b(correction|corrected)\s+(to|from)\s*:?[^.]{0,160}\.?\s*/gi, " ");
  t = t.replace(/\bupdated\s+location\s*:?\s*[^.\n]{0,120}\.?\s*/gi, " ");
  t = t.replace(/\s+/g, " ").replace(/\s+\./g, ".").trim();
  return t;
}

function dedupeIssueParagraphsAggressive(text) {
  const paras = String(text || "")
    .split(/\n\n+/)
    .map((p) => p.trim())
    .filter(Boolean);
  if (paras.length <= 1) return text;
  const kept = [];
  const sigs = [];
  for (const p of paras) {
    const n = norm(p);
    if (n.length < 12) {
      kept.push(p);
      continue;
    }
    const sig = n.slice(0, 88);
    let dup = false;
    for (const prev of sigs) {
      if (sig === prev || (sig.length > 28 && prev.length > 28 && (sig.includes(prev.slice(0, 50)) || prev.includes(sig.slice(0, 50))))) {
        dup = true;
        break;
      }
    }
    if (dup) continue;
    sigs.push(sig);
    kept.push(p);
  }
  return kept.length ? kept.join("\n\n") : text;
}

function buildCaptionDedupHaystack(merged) {
  return norm(
    [
      merged.execSummary,
      merged.issuesText,
      merged.workNarrative,
      merged.concreteNarrative,
      merged.openIntro,
      ...(merged.workSectionsAi || []).flatMap((s) => s.items || []),
    ]
      .filter(Boolean)
      .join("\n")
  );
}

function filterWeakOpenItemRows(openItemsTableRaw, merged) {
  if (!openItemsTableRaw || !String(openItemsTableRaw).trim()) return openItemsTableRaw;
  const hay = norm(
    [merged.issuesText, merged.execSummary, merged.workNarrative, merged.concreteNarrative].filter(Boolean).join("\n")
  );
  const rows = String(openItemsTableRaw).split(/\r?\n/).filter(Boolean);
  const kept = rows.filter((row) => {
    const parts = row.split("|");
    const action = String(parts[1] != null ? parts[1] : "").trim();
    if (action.length < 20) return true;
    const an = norm(action);
    const slice = an.slice(0, Math.min(100, an.length));
    if (hay.includes(slice)) return false;
    return true;
  });
  return kept.length ? kept.join("\n") : openItemsTableRaw;
}

function filterOpenIntro(openIntro, merged) {
  const intro = String(openIntro || "").trim();
  if (!intro || intro === "â€”") return openIntro;
  const hay = norm(
    [
      merged.issuesText,
      merged.execSummary,
      merged.workNarrative,
      merged.concreteNarrative,
      merged.openItemsTableRaw ? String(merged.openItemsTableRaw).replace(/\|/g, " ") : "",
    ]
      .filter(Boolean)
      .join("\n")
  );
  if (intro.length >= 24 && paragraphContainedInHaystack(intro, hay)) return "";
  return openIntro;
}

function filterConcreteNarrative(concreteNarrative, merged) {
  const c = String(concreteNarrative || "").trim();
  if (!c || c === "—") return concreteNarrative;
  const openBits = merged.openItemsTableRaw
    ? norm(String(merged.openItemsTableRaw).replace(/\|/g, " "))
    : "";
  const hay = norm(
    [
      merged.execSummary,
      merged.workNarrative,
      merged.issuesText,
      buildWorkHaystack(merged),
      openBits,
    ].join("\n")
  );
  if (c.length >= 30 && paragraphContainedInHaystack(c, hay)) return "—";
  return concreteNarrative;
}

function dedupeLooseLines(lines) {
  const out = [];
  const seenNorm = [];
  for (const line of lines || []) {
    const t = String(line || "").trim();
    if (!t) continue;
    const n = norm(t);
    if (!n) continue;
    let dup = false;
    for (const prev of seenNorm) {
      if (n === prev) {
        dup = true;
        break;
      }
      if (n.length >= 28 && prev.length >= 28) {
        if (n.includes(prev) || prev.includes(n)) {
          dup = true;
          break;
        }
      }
    }
    if (dup) continue;
    seenNorm.push(n);
    out.push(t);
  }
  return out;
}

/**
 * Issue chunk body line is redundant if the PDF issues block already contains the same wording.
 */
function issueChunkLineRedundant(chunkText, mergedIssuesText) {
  const t = norm(chunkText);
  if (t.length < 18) return false;
  const iss = norm(mergedIssuesText || "");
  return iss.includes(t.slice(0, Math.min(100, t.length)));
}

function collectIssuePhotoMediaIds(st) {
  const s = new Set();
  for (const ch of st.issueChunks || []) {
    for (const p of ch.photos || []) {
      if (p && p.mediaId != null) s.add(String(p.mediaId));
    }
  }
  return s;
}

function filterPhotosNotForIssues(list, issueIds) {
  if (!issueIds || issueIds.size === 0) return list || [];
  return (list || []).filter((p) => p && !issueIds.has(String(p.mediaId)));
}

function dedupePhotosByMediaId(photos) {
  const seen = new Set();
  const out = [];
  for (const p of photos || []) {
    const id = p && p.mediaId != null ? String(p.mediaId) : "";
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(p);
  }
  return out;
}

function captionKey(p) {
  const c = String(p.captionText || "").trim().toLowerCase().slice(0, 72);
  return c.replace(/\s+/g, " ") || "__empty__";
}

/** Keep at most maxPerKey photos that share the same caption prefix (near-duplicates). */
function capPhotosBySimilarCaption(photos, maxPerKey) {
  const counts = new Map();
  const out = [];
  for (const p of photos || []) {
    const k = captionKey(p);
    const n = (counts.get(k) || 0) + 1;
    counts.set(k, n);
    if (n <= maxPerKey) out.push(p);
  }
  return out;
}

function sortPhotosForRender(photos) {
  const list = [...(photos || [])];
  list.sort((a, b) => {
    const ia = a.includeInDailyReport === true ? 1 : 0;
    const ib = b.includeInDailyReport === true ? 1 : 0;
    if (ib !== ia) return ib - ia;
    const pa = a.linkedLogEntryId ? 1 : 0;
    const pb = b.linkedLogEntryId ? 1 : 0;
    if (pb !== pa) return pb - pa;
    const ca = String(a.captionText || "").length;
    const cb = String(b.captionText || "").length;
    return cb - ca;
  });
  return list;
}

/**
 * Shorten caption if it repeats the preceding log line or global report copy; keep timestamps.
 */
function refineCaptionForPdf(captionBody, contextLine, globalHayNorm) {
  let c = String(captionBody || "").trim();
  const ctx = String(contextLine || "").trim();
  if (!c) return "";
  const cn = norm(c);
  if (ctx.length > 15) {
    const xn = norm(ctx);
    if (cn.length > 22 && xn.includes(cn.slice(0, Math.min(90, cn.length)))) return "";
  }
  if (globalHayNorm && cn.length > 22 && globalHayNorm.includes(cn.slice(0, Math.min(75, cn.length)))) {
    return "";
  }
  if (c.length > 88) return `${c.slice(0, 85)}...`;
  return c;
}

/**
 * @param {object} merged — merged PDF payload (may be mutated)
 * @param {object} det — model.deterministic
 */
function prepareMergedForPdf(merged, det) {
  if (!merged) return merged;
  merged.execSummary = stripSourceLogArtifacts(merged.execSummary || "");
  merged.issuesText = stripSourceLogArtifacts(merged.issuesText || "");
  merged.workNarrative = stripSourceLogArtifacts(merged.workNarrative || "");
  merged.concreteNarrative = stripSourceLogArtifacts(merged.concreteNarrative || "");
  merged.openIntro = stripSourceLogArtifacts(merged.openIntro || "");

  merged.issuesText = filterIssuesText(merged.issuesText, merged);
  merged.issuesText = dedupeIssueParagraphsAggressive(merged.issuesText);

  if (shouldOmitManpowerNarrativeV2(merged, det)) {
    merged.manpowerNarrative = "";
  } else {
    merged.manpowerNarrative = stripSourceLogArtifacts(merged.manpowerNarrative || "");
  }

  merged.openItemsTableRaw = filterWeakOpenItemRows(merged.openItemsTableRaw, merged);
  merged.openIntro = filterOpenIntro(merged.openIntro, merged);

  merged.concreteNarrative = filterConcreteNarrative(merged.concreteNarrative, merged);
  merged.concreteNarrative = stripSourceLogArtifacts(merged.concreteNarrative || "");

  if (merged.workSectionsAi && merged.workSectionsAi.length) {
    merged.workSectionsAi = merged.workSectionsAi.map((s) => ({
      ...s,
      items: dedupeLooseLines((s.items || []).map((it) => stripSourceLogArtifacts(String(it || "")))),
    }));
  }

  return merged;
}

module.exports = {
  prepareMergedForPdf,
  collectIssuePhotoMediaIds,
  filterPhotosNotForIssues,
  dedupePhotosByMediaId,
  capPhotosBySimilarCaption,
  sortPhotosForRender,
  refineCaptionForPdf,
  issueChunkLineRedundant,
  stripSourceLogArtifacts,
  filterManpowerChunksForPdf,
  buildCaptionDedupHaystack,
};
