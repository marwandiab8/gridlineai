<!-- developer-dashboard-codex-ingestion:start -->
## Report completed Codex sessions to Developer Dashboard

At the end of a successful coding session:

1. Determine this repository's strongest stable Developer Dashboard identity: explicit Dashboard project UUID, immutable GitHub numeric repository ID, or exact GitHub owner/repository full name. Never guess from a project title.
2. Build one strict Codex session ingestion V1 JSON object. Use a stable externalSessionId for this Codex session and reuse the identical ID and payload for retries.
3. Include the actual driving prompt, objective, summary, completed and unfinished work, discovered problems, implementation decisions, modified files, commits, branch, explicit blocker, next recommended task, ideas, and start/end timestamps. Use empty arrays when a required list has no items. Never include credentials, tokens, authorization headers, private keys, service-account data, or secret values.
4. Save the payload outside the repository or pipe it directly to the shared helper. Run:

   node /home/marwan/Documents/developer-dashboard/tools/report-codex-session.mjs complete --file /path/to/codex-session.json

5. The helper must read DEVELOPER_DASHBOARD_CODEX_INGEST_URL and DEVELOPER_DASHBOARD_CODEX_INGEST_TOKEN from the process environment. Never read, print, log, inspect, request, or pass the credential as a command argument.
6. Treat a successful helper response, including an already-recorded idempotent response, as reported. If ingestion fails, report the sanitized failure to the user but do not undo, reset, amend, or otherwise change the completed coding work.
7. Do not fuzzy-match, create a Dashboard project automatically, change the external session ID to evade a conflict, or claim ingestion succeeded without a successful helper response.
<!-- developer-dashboard-codex-ingestion:end -->
