# Codex Status — GridlineAI

Date: 2026-08-12

## Repository

- Repository: `/home/marwan/Documents/ChatBot`
- Branch: `main`
- `HEAD` and `origin/main`: `fe585eefefd3af8a169992a15823f115c838acc9`
- The worktree intentionally contains the accumulated field-work, reporting, UI, Firebase, iOS Shortcuts, TimeLeft, Procore, and labour-parser changes.
- Generated Firebase hosting cache and local runtime files remain excluded from any commit.

## Labour parser

- `functions/labourRepository.js` matches the deployed parser SHA-256 `fe64c18311776d9d1b4929f761692ef661628f22dbf61108974559ba18b99ab4`.
- Shawn Jones’s exact field message parses to 8.5 hours with four normalized task segments.
- Production entry `LFQYpOz7dzISvJZFnHhl` remains at 510 minutes (8.5 hours), report date `2026-08-11`, project `docksteader`, with original notes preserved.
- Labour report `SW2afjk2RBU1XN7qyE3r` was not regenerated.

## Security and integration review

- Procore login, status, selection, and data routes require verified management access; OAuth callback state is cookie-bound and transactionally consumed; callback HTML escapes external text; tokens remain server-side; production mode is fail-closed and the integration is disabled by default.
- iOS Shortcut project ownership is resolved from authorized project assignments; unassigned tracking events are excluded from project-specific reports; idempotency-key claims use a Firestore transaction.
- TimeLeft delivery remains explicitly disabled when configuration is absent and preserves durable source-event state.

## Validation and remaining blocker

- Functions tests and syntax/JSON checks pass locally.
- `PROCORE_CLIENT_SECRET` must be provisioned in Firebase Secret Manager before deploying Procore functions.
- The broad Firebase dry run currently stops because that secret version is absent.
- The localhost Procore smoke test could not run because the sandbox prohibits listening sockets.
- These are deployment/environment prerequisites, not source-test failures: the local automated Functions suite passed 30/30 test files, and syntax/JSON checks passed.
- Creating or rotating that secret is intentionally outside this worktree-only session. No deployment, remote configuration change, Firestore write, push, or PR was performed.
