#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_TARGET="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_SOURCE="/home/marwan/Documents/aigridline"

SOURCE_REPO="${SOURCE_REPO:-$DEFAULT_SOURCE}"
TARGET_REPO="${TARGET_REPO:-$DEFAULT_TARGET}"

usage() {
  cat <<EOF
Usage:
  scripts/project-sync.sh summary
  scripts/project-sync.sh diff <relative-path>
  scripts/project-sync.sh copy <relative-path> [more paths...]
  scripts/project-sync.sh copy-changed
  scripts/project-sync.sh list-changed

Environment overrides:
  SOURCE_REPO=/abs/path/to/source
  TARGET_REPO=/abs/path/to/target

Default source: ${DEFAULT_SOURCE}
Default target: ${DEFAULT_TARGET}
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

require_repo() {
  local repo="$1"
  [[ -d "$repo" ]] || die "Repo path not found: $repo"
  git -C "$repo" rev-parse --show-toplevel >/dev/null 2>&1 || die "Not a git repo: $repo"
}

print_header() {
  echo
  echo "== $* =="
}

list_changed() {
  git -C "$SOURCE_REPO" status --short \
    | awk '{print $2}' \
    | sed '/^$/d'
}

copy_one() {
  local rel="$1"
  local src="$SOURCE_REPO/$rel"
  local dst="$TARGET_REPO/$rel"

  [[ -f "$src" ]] || die "Source file not found: $src"
  mkdir -p "$(dirname "$dst")"
  cp "$src" "$dst"
  echo "Copied: $rel"
}

cmd_summary() {
  require_repo "$SOURCE_REPO"
  require_repo "$TARGET_REPO"

  print_header "Source Repo"
  echo "$SOURCE_REPO"
  print_header "Target Repo"
  echo "$TARGET_REPO"

  print_header "Source Working Tree"
  git -C "$SOURCE_REPO" status --short || true

  print_header "Target Working Tree"
  git -C "$TARGET_REPO" status --short || true

  print_header "Changed Files In Source"
  local changed
  changed="$(list_changed || true)"
  if [[ -z "$changed" ]]; then
    echo "No uncommitted source changes."
    return 0
  fi
  printf '%s\n' "$changed"

  print_header "Cross-Repo Match Check"
  while IFS= read -r rel; do
    [[ -n "$rel" ]] || continue
    if [[ ! -e "$TARGET_REPO/$rel" ]]; then
      echo "missing-in-target  $rel"
      continue
    fi
    if cmp -s "$SOURCE_REPO/$rel" "$TARGET_REPO/$rel"; then
      echo "same             $rel"
    else
      echo "different        $rel"
    fi
  done <<< "$changed"
}

cmd_diff() {
  local rel="${1:-}"
  [[ -n "$rel" ]] || die "diff requires a relative path"
  [[ -e "$SOURCE_REPO/$rel" ]] || die "Missing in source: $rel"
  [[ -e "$TARGET_REPO/$rel" ]] || die "Missing in target: $rel"
  git --no-pager diff --no-index -- "$TARGET_REPO/$rel" "$SOURCE_REPO/$rel" || true
}

cmd_copy() {
  [[ "$#" -gt 0 ]] || die "copy requires at least one relative path"
  local rel
  for rel in "$@"; do
    copy_one "$rel"
  done

  print_header "Target Working Tree"
  git -C "$TARGET_REPO" status --short -- "$@"
}

cmd_copy_changed() {
  local changed
  changed="$(list_changed || true)"
  [[ -n "$changed" ]] || die "No uncommitted source changes to copy"
  while IFS= read -r rel; do
    [[ -n "$rel" ]] || continue
    copy_one "$rel"
  done <<< "$changed"

  print_header "Target Working Tree"
  while IFS= read -r rel; do
    [[ -n "$rel" ]] || continue
    git -C "$TARGET_REPO" status --short -- "$rel"
  done <<< "$changed"
}

main() {
  local cmd="${1:-}"
  shift || true

  require_repo "$SOURCE_REPO"
  require_repo "$TARGET_REPO"

  case "$cmd" in
    summary)
      cmd_summary
      ;;
    diff)
      cmd_diff "$@"
      ;;
    copy)
      cmd_copy "$@"
      ;;
    copy-changed)
      cmd_copy_changed
      ;;
    list-changed)
      list_changed
      ;;
    ""|-h|--help|help)
      usage
      ;;
    *)
      die "Unknown command: $cmd"
      ;;
  esac
}

main "$@"
