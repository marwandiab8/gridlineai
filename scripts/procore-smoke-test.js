"use strict";

const url = process.env.PROCORE_STATUS_URL || "http://localhost:3000/api/procore/status";

async function main() {
  const token = String(process.env.PROCORE_FIREBASE_ID_TOKEN || "").trim();
  const response = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  const body = await response.text();
  let data;
  try {
    data = JSON.parse(body);
  } catch (_) {
    throw new Error(`Expected JSON from ${url}, got: ${body.slice(0, 300)}`);
  }

  if (token) {
    if (!response.ok) throw new Error(`Authenticated Procore status check failed (${response.status}).`);
    if (data.ok === false) throw new Error("Authenticated Procore status returned an error.");
  } else if (response.status !== 401 && response.status !== 403) {
    throw new Error(`Unauthenticated Procore status check expected 401/403, got ${response.status}.`);
  }
  console.log(token ? "Authenticated Procore status check passed." : "Unauthenticated Procore status correctly rejected.");
}

main().catch((err) => {
  console.error(err.message);
  process.exitCode = 1;
});
