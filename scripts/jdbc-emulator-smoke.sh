#!/usr/bin/env bash
set -euo pipefail
if [[ $# != 2 || ! -f "$1" ]]; then
  echo 'Usage: bash scripts/jdbc-emulator-smoke.sh <packaged-driver.jar> <loopback-endpoint>' >&2
  exit 2
fi
script_dir="$(cd "$(dirname "$0")" && pwd)"
classes="$(mktemp -d)"
trap 'rm -rf "$classes"' EXIT
javac -source 8 -target 8 -cp "$1" -d "$classes" "$script_dir/JdbcEmulatorSmoke.java"
java -cp "$classes:$1" JdbcEmulatorSmoke "$2"
