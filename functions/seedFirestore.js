/**
 * Creates starter Firestore docs for the Construction SMS assistant.
 *
 * Run (PowerShell):
 *   cd c:\Users\creat\Documents\ChatBot\functions
 *   npm run seed
 *
 * Auth (pick one) — "firebase login" alone does NOT work for this script:
 *   - gcloud auth application-default login
 *     gcloud config set project gridlineai
 *   - Or set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON
 *     (role: Cloud Datastore User or Editor on the project)
 *
 * Prerequisite: Firestore database exists (Firebase Console → Firestore → Create database, Production mode).
 */

const admin = require("firebase-admin");
const { FieldValue } = require("firebase-admin/firestore");

const PROJECT_ID =
  process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || "gridlineai";

if (!admin.apps.length) {
  admin.initializeApp({ projectId: PROJECT_ID });
}

const db = admin.firestore();

async function seed() {
  const companyRef = db.collection("adminSettings").doc("company");
  await companyRef.set(
    {
      companyStandards:
        "Stop work for imminent danger. PPE per site orientation. Report near-misses to the super.",
      responseStyle:
        "Short SMS. Plain English. Direct. A little site slang is OK when it saves words.",
      approvedTerminology:
        "Use 'pour' for concrete placement in texts when it sounds natural.",
      reportingPreferences:
        "Encourage logging issues and deliveries with log issue: / log delivery: when they affect schedule.",
      escalationRules:
        "Life safety: call 911 and site safety. Quality hold: notify PM same day.",
      seededAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  const projRef = db.collection("projects").doc("docksteader");
  await projRef.set(
    {
      name: "Docksteader (sample)",
      instructionText:
        "Sample project — replace with real GC rules, spec references, and job-specific notes before production.",
      contactsText:
        "Super: add name/phone. Safety: site trailer. PM: add contact.",
      scheduleNotes:
        "3-week lookahead: add key milestones (pours, inspections, critical deliveries).",
      faqText: "Laydown area: TBD. Gate hours: TBD.",
      notes: "Created by npm run seed — safe to edit or delete in Console.",
      seededAt: FieldValue.serverTimestamp(),
    },
    { merge: true }
  );

  console.log("Done.");
  console.log("  adminSettings/company");
  console.log("  projects/docksteader");
  console.log("Text your Twilio number: project docksteader");
}

seed()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("\nSeed failed:", e.message || e);
    console.error(`
Fix: use ONE of these, then run npm run seed again:
  1) gcloud auth application-default login
     gcloud config set project ${PROJECT_ID}
  2) $env:GOOGLE_APPLICATION_CREDENTIALS="C:\\path\\to\\serviceAccount.json"

Or add the documents manually in Firebase Console (see firestore/seed-documents.json in the repo).
`);
    process.exit(1);
  });
