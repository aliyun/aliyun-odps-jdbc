#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 1 || ! -f "$1" ]]; then
  echo "Usage: bash scripts/jdbc-core-smoke.sh <packaged-driver.jar>" >&2
  exit 2
fi
for name in ALIBABA_CLOUD_ACCESS_KEY_ID ALIBABA_CLOUD_ACCESS_KEY_SECRET MAXCOMPUTE_PROJECT MAXCOMPUTE_ENDPOINT; do
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: $name" >&2
    exit 2
  fi
done
script_dir="$(cd "$(dirname "$0")" && pwd)"
classes="$(mktemp -d)"
trap 'rm -rf "$classes"' EXIT
javac -source 8 -target 8 -d "$classes" "$script_dir/JdbcCoreSmoke.java"
java -cp "$classes:$1" JdbcCoreSmoke
