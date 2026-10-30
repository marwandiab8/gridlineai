// One-off: set appMembers/marwandiab8@gmail.com.etaNotify for the "Hey Siri, leaving work" text (Oct 30, 2026).
// Dry run unless --apply. Run from functions/:
//   node ../scripts/set-eta-notify-2026-10-30.cjs --contact=+1XXXXXXXXXX --home=43.70,-80.39 --work=43.75,-79.79            (shows the plan)
//   node ../scripts/set-eta-notify-2026-10-30.cjs --contact=+1XXXXXXXXXX --home=43.70,-80.39 --work=43.75,-79.79 --apply    (backs up, then writes)
// --contact gets the text: your own phone for the first test, then Ashley's (docs/leaving-work-eta.md).
// --home / --work can be left off once set; the stored values are kept. --work is the Docksteader site, the
// origin of last resort when a run has neither the Shortcut's location nor a recent OwnTracks fix.
// The appMembers document is written to /home/marwan/Documents/backups/eta-notify-2026-10-30-<time>.json first.
const fs = require("fs");
const admin = require(process.cwd() + "/node_modules/firebase-admin");
const { haversineMeters } = require(process.cwd() + "/placeLearning");

const APPLY = process.argv.includes("--apply");
const arg = (name) => {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3).trim() : null;
};
const EMAIL = "marwandiab8@gmail.com";
const db = admin.initializeApp({ projectId: "gridlineai" }).firestore();
const ser = (o) =>
  JSON.parse(JSON.stringify(o, (k, v) => (v && v._seconds != null ? { __ts: new Date(v._seconds * 1000 + Math.round(v._nanoseconds / 1e6)).toISOString() } : v)));

(async () => {
  const ref = db.collection("appMembers").doc(EMAIL);
  const snap = await ref.get();
  if (!snap.exists) throw new Error(`missing ${ref.path}`);
  const member = snap.data();
  if (!(member.shortcutIntegration && member.shortcutIntegration.tokenHash)) throw new Error("not the Shortcut member (no tokenHash)");
  const current = member.etaNotify || {};

  const contact = arg("contact") || current.contactPhoneE164;
  if (!/^\+1\d{10}$/.test(String(contact || ""))) throw new Error(`--contact must be +1 and 10 digits (got ${contact || "nothing"})`);
  const point = (name, stored) => {
    if (!arg(name)) return stored || null;
    const [latitude, longitude] = arg(name).split(",").map(Number);
    if (!(latitude >= -90 && latitude <= 90 && longitude >= -180 && longitude <= 180)) throw new Error(`bad --${name} ${arg(name)}`);
    return { latitude, longitude };
  };
  const home = point("home", current.home);
  const work = point("work", current.work);
  if (!home || !work) throw new Error("pass --home=lat,lng and --work=lat,lng");

  const next = {
    enabled: true,
    contactName: "Ashley",
    contactPhoneE164: contact,
    firstName: "Marwan",
    home,
    work,
    minGapMinutes: 45,
    fallback: { enabled: true, afterLocalHour: 15, waitMinutes: 8, minProgressMeters: 4000, expireMinutes: 60 },
  };
  console.log("before:", JSON.stringify(current));
  console.log("after: ", JSON.stringify(next));
  console.log(`contact is ${contact === member.approvedPhoneE164 ? "YOUR OWN phone (test)" : "not your phone"}`);
  console.log(`site -> home straight line: ${(haversineMeters(work.latitude, work.longitude, home.latitude, home.longitude) / 1000).toFixed(1)} km`);
  if (!APPLY) return console.log("dry run; add --apply to write");

  const backup = `/home/marwan/Documents/backups/eta-notify-2026-10-30-${new Date().toISOString().replace(/[:.]/g, "-")}.json`;
  fs.writeFileSync(backup, JSON.stringify({ [ref.path]: ser(member) }, null, 2));
  await ref.update({ etaNotify: next });
  console.log(`written; backup at ${backup}`);
})().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
