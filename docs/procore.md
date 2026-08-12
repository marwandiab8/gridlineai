# Procore OAuth Setup

This repository uses Firebase Hosting plus Cloud Functions. The live GridlineAI Procore redirect URI is:

```text
https://gridlineai.web.app/callback
```

The local emulator Procore redirect URI is:

```text
http://localhost:3000/callback
```

Add each URI you plan to use to the matching OAuth Credentials redirect URI list in the Procore Developer Portal, one URI per line. Use Sandbox OAuth Credentials for sandbox and Production OAuth Credentials for real Procore data.

## Environment

For local development, use sandbox credentials only. Put these values in `functions/.env` when running Firebase emulators locally:

```dotenv
PROCORE_ENABLED=false
PROCORE_ENV=sandbox
PROCORE_CLIENT_ID=your_sandbox_client_id
PROCORE_CLIENT_SECRET=your_sandbox_client_secret
PROCORE_REDIRECT_URI=http://localhost:3000/callback
PROCORE_AUTH_BASE_URL=https://login-sandbox.procore.com
PROCORE_API_BASE_URL=https://sandbox.procore.com
PROCORE_COMPANY_ID=4286302
PROCORE_PROJECT_NAME=Sandbox Test Project
```

Do not put production credentials in local sandbox files. Procore is disabled by default; an explicitly approved sandbox setup must set `PROCORE_ENABLED=true`. Production is fail-closed until encrypted token storage is approved. `PROCORE_CLIENT_SECRET` is bound through Firebase Secret Manager, never committed as ordinary configuration.

For production discovery, start with your production OAuth credentials and leave company/project values blank until `/rest/v1.0/companies` returns the real company id:

```dotenv
PROCORE_ENV=production
PROCORE_CLIENT_ID=your_production_client_id
PROCORE_CLIENT_SECRET=your_production_client_secret
PROCORE_REDIRECT_URI=https://gridlineai.web.app/callback
PROCORE_AUTH_BASE_URL=https://login.procore.com
PROCORE_API_BASE_URL=https://api.procore.com
PROCORE_COMPANY_ID=
PROCORE_PROJECT_NAME=
```

For the deployed GridlineAI function environment, use:

```dotenv
PROCORE_REDIRECT_URI=https://gridlineai.web.app/callback
```

## Run Locally

Install function dependencies if needed:

```bash
cd functions
npm ci
```

Start the Firebase emulators from the repo root:

```bash
firebase emulators:start --only hosting,functions,firestore
```

Hosting is configured on port `3000`, so open:

```text
http://localhost:3000/procore
```

After signing into the GridlineAI dashboard with management access, use the Connect Procore control:

```text
http://localhost:3000/api/procore/login
```

## Verify

After OAuth succeeds, the callback stores tokens server-side in Firestore under function-only collections. The browser never receives access or refresh tokens.

Use the status page or call:

```text
http://localhost:3000/api/procore/status
```

The authenticated status endpoint calls:

- `GET /rest/v1.0/me`
- `GET /rest/v1.0/companies`
- `GET /rest/v1.1/projects?company_id=<PROCORE_COMPANY_ID>` when `PROCORE_COMPANY_ID` is configured

In production, first use `/rest/v1.0/companies` to find the real company id. Then set `PROCORE_COMPANY_ID`, call `/rest/v1.1/projects?company_id=<company_id>`, and use the returned API project `id`. A visible project number is not the same thing as the API project id.

## 403 Checks

If Procore returns `403 Forbidden`, check that the custom app is installed in the target company/project and that the app version key and permissions are configured in Procore.

## Smoke Test

With emulators running and OAuth completed:

```bash
cd functions
npm run procore:test
```

## Protected Procore Data Reads

After OAuth is connected, approved GridlineAI management/admin users can call a protected data read endpoint:

```text
GET /api/procore/data?path=/rest/v1.1/projects&company_id=4286302
Authorization: Bearer <firebase_id_token>
```

This endpoint:

- accepts only `GET`
- accepts only Procore paths beginning with `/rest/`
- attaches the stored Procore OAuth access token server-side
- never returns Procore access or refresh tokens
- is intended as the foundation for Procore exports, sync jobs, and data manipulation workflows

Use it first to inspect Procore responses, then build specific sync routines for RFIs, observations, punch items, daily logs, documents/photos, and reports.
