# Project Sync Workflow

Use this repo-local helper to compare `aigridline` and `gridlineai` and port fixes deliberately.

Script:

```bash
scripts/project-sync.sh
```

Defaults:

- `SOURCE_REPO=/home/marwan/Documents/aigridline`
- `TARGET_REPO=/home/marwan/Documents/ChatBot`

Commands:

```bash
scripts/project-sync.sh summary
scripts/project-sync.sh list-changed
scripts/project-sync.sh diff functions/dailyReportWeather.js
scripts/project-sync.sh copy functions/dailyReportWeather.js functions/dailyReportWeather.test.js
scripts/project-sync.sh copy-changed
```

Recommended workflow:

1. Make and verify the fix in the source project.
2. Run `scripts/project-sync.sh summary` to see which source files changed and whether the matching file in the target differs.
3. Run `scripts/project-sync.sh diff <relative-path>` for each candidate file and confirm the change is portable.
4. Copy only the files you actually want with `scripts/project-sync.sh copy <relative-path>`.
5. Run the relevant tests in the target project.

Notes:

- Paths are repo-relative, for example `functions/dailyReportPdf.js`.
- `copy-changed` copies every uncommitted file from the source repo into the target repo. Use that only when the entire source working tree is intended to move across.
- To reverse direction, override the env vars:

```bash
SOURCE_REPO=/home/marwan/Documents/ChatBot \
TARGET_REPO=/home/marwan/Documents/aigridline \
scripts/project-sync.sh summary
```
