#!/usr/bin/env bash
# Run the consumer compatibility probe (scripts/JdkConsumerMatrix.java) against one packaged
# driver jar on one or more JVMs, then report per-JVM results and cross-JVM divergences.
#
# Usage:
#   bash scripts/jdk-consumer-matrix.sh <packaged-driver.jar> [JAVA_HOME ...]
#
# With no JAVA_HOME arguments the ambient java/javac is used, which is what a CI matrix cell
# wants. Pass several JDK homes (for example the JDKs installed on a workstation) to get the
# whole matrix in one run. Live SQL cells need the same environment as scripts/jdbc-core-smoke.sh
# (ALIBABA_CLOUD_ACCESS_KEY_ID, ALIBABA_CLOUD_ACCESS_KEY_SECRET, MAXCOMPUTE_PROJECT,
# MAXCOMPUTE_ENDPOINT); without them those cells report SKIPPED and the run still passes.
#
# Exit status: 0 only if every cell terminated cleanly, no check failed and no observable
# diverges between JVMs. A JVM that cannot exit on its own is reported as a timeout, not a pass.
set -u -o pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/jdk-consumer-matrix.sh <packaged-driver.jar> [JAVA_HOME ...]" >&2
  exit 2
fi
DRIVER_JAR="$1"
shift || true
if [[ ! -f "$DRIVER_JAR" ]]; then
  echo "Packaged driver jar not found: $DRIVER_JAR" >&2
  exit 2
fi
DRIVER_JAR="$(cd "$(dirname "$DRIVER_JAR")" && pwd)/$(basename "$DRIVER_JAR")"

script_dir="$(cd "$(dirname "$0")" && pwd)"
probe_source="$script_dir/JdkConsumerMatrix.java"
timeout_seconds="${MATRIX_TIMEOUT:-300}"
# Observables that legitimately vary per JVM and are therefore not compared.
parity_ignore='^runtime_|^offline_connect_latency_bucket$|^elapsed_ms_bucket$|^checks_total$|^checks_failed$'

work_dir="$(mktemp -d)"
cleanup() { rm -rf "$work_dir"; }
trap cleanup EXIT

jdks=()
if [[ $# -gt 0 ]]; then
  for candidate in "$@"; do jdks+=("$candidate"); done
else
  jdks+=("${JAVA_HOME:-/usr}")
fi

declare -a labels results
overall_rc=0

run_cell() {
  local label="$1" java_bin="$2" javac_bin="$3" classes="$4"
  local out="$work_dir/$label.txt" rc=0
  timeout "$timeout_seconds" "$java_bin" -cp "$classes:$DRIVER_JAR" JdkConsumerMatrix > "$out" 2>"$work_dir/$label.err"
  rc=$?
  if [[ $rc -eq 124 ]]; then
    echo "TIMEOUT $label: JVM still alive after ${timeout_seconds}s; a non-daemon thread is keeping a consumer JVM running" >&2
    overall_rc=1
  elif [[ $rc -ne 0 ]]; then
    echo "FAILED  $label: probe exited $rc" >&2
    overall_rc=1
  fi
  if [[ -s "$work_dir/$label.err" ]]; then
    echo "stderr  $label:" >&2
    sed -e 's/^/    /' "$work_dir/$label.err" >&2
  fi
  if [[ -n "${MATRIX_RESULTS_DIR:-}" ]]; then
    mkdir -p "$MATRIX_RESULTS_DIR"
    cp "$out" "$MATRIX_RESULTS_DIR/$label.txt"
    if [[ -s "$work_dir/$label.err" ]]; then
      cp "$work_dir/$label.err" "$MATRIX_RESULTS_DIR/$label.err"
    fi
  fi
  labels+=("$label")
  results+=("$out")
}

echo "== packaged driver: $(basename "$DRIVER_JAR") ($(stat -c %s "$DRIVER_JAR" 2>/dev/null || stat -f %z "$DRIVER_JAR") bytes)"
for index in "${!jdks[@]}"; do
  jdk="${jdks[$index]}"
  java_bin="$jdk/bin/java"
  javac_bin="$jdk/bin/javac"
  if [[ "$jdk" == "/usr" ]]; then
    java_bin="$(command -v java || true)"
    javac_bin="$(command -v javac || true)"
  fi
  if [[ -z "$java_bin" || ! -x "$java_bin" ]]; then
    echo "Missing java under: $jdk (checked $java_bin)" >&2
    overall_rc=1
    continue
  fi
  label="jdk-$index-$(basename "$jdk")"
  version="$("$java_bin" -version 2>&1 | head -1)"
  classes="$work_dir/classes-$index"
  mkdir -p "$classes"
  echo "-- cell $label: $version"
  if [[ ! -x "$javac_bin" ]]; then
    echo "FAILED  $label: no javac under $jdk (a JDK home is required, a JRE is not enough)" >&2
    overall_rc=1
    continue
  fi
  # Consumers compile against the driver, so the probe is built with this JVM's own javac too.
  if ! "$javac_bin" -nowarn -source 8 -target 8 -d "$classes" -cp "$DRIVER_JAR" "$probe_source" \
      2>"$work_dir/$label.javac"; then
    echo "FAILED  $label: probe does not compile against the packaged jar on this JVM" >&2
    sed -e 's/^/    /' "$work_dir/$label.javac" | grep -v 'bootstrap class path\|deprecat' >&2
    overall_rc=1
    continue
  fi
  run_cell "$label" "$java_bin" "$javac_bin" "$classes"
  if grep -q '^MATRIX_CELL_RESULT=PASS$' "${results[-1]}"; then
    echo "PASS    $label"
  else
    echo "FAILED  $label: $(grep '^MATRIX_CELL_RESULT=' "${results[-1]}" || echo 'no result line')"
    overall_rc=1
  fi
  grep -E '^(FAILED|FINDING) ' "${results[-1]}" || true
done

if [[ ${#labels[@]} -ge 2 ]]; then
  echo "== cross-JVM parity (baseline: ${labels[0]})"
  baseline="${results[0]}"
  # Every compared observable in the baseline file, minus the ones that legitimately vary per JVM.
  mapfile -t baseline_keys < <(grep -E '^[a-z][a-z0-9_]*=' "$baseline" \
    | cut -d= -f1 | grep -Ev "$parity_ignore" | sort -u)
  for index in "${!labels[@]}"; do
    [[ $index -eq 0 ]] && continue
    other="${labels[$index]}"
    other_file="${results[$index]}"
    divergence_count=0
    for key in "${baseline_keys[@]}"; do
      baseline_value="$(sed -n "s/^${key}=//p" "$baseline" | head -1)"
      other_value="$(sed -n "s/^${key}=//p" "$other_file" | head -1)"
      if [[ -z "$other_value" ]]; then
        echo "INCOMPATIBILITY $key: reported by ${labels[0]} but missing on $other" >&2
        divergence_count=$((divergence_count + 1))
      elif [[ "$baseline_value" != "$other_value" ]]; then
        echo "INCOMPATIBILITY $key: ${labels[0]}=$baseline_value $other=$other_value" >&2
        divergence_count=$((divergence_count + 1))
      fi
    done
    # Keys that only exist on the other JVM are divergences too (a code path that ran there only).
    for key in $(grep -E '^[a-z][a-z0-9_]*=' "$other_file" | cut -d= -f1 | grep -Ev "$parity_ignore" | sort -u); do
      if ! grep -q "^${key}=" "$baseline"; then
        echo "INCOMPATIBILITY $key: reported by $other but missing on ${labels[0]}" >&2
        divergence_count=$((divergence_count + 1))
      fi
    done
    if [[ $divergence_count -eq 0 ]]; then
      echo "same      $other: every compared observable matches ${labels[0]}"
    else
      overall_rc=1
    fi
  done
fi

echo "== matrix rc=$overall_rc"
exit "$overall_rc"
