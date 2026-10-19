#!/usr/bin/env python3
"""Stress the shaded JDBC artifact against a loopback emulator in bounded JVMs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("jar", type=Path)
parser.add_argument("--endpoint", default="http://127.0.0.1:18080")
parser.add_argument("--output", type=Path, default=Path("target/sql-performance"))
parser.add_argument("--profile", choices=["ci", "baseline"], default="ci")
parser.add_argument("--case-timeout", type=int, default=120)
parser.add_argument("--driver-commit", help="Verified source commit of the supplied artifact, when known")
args = parser.parse_args()
if args.driver_commit and not re.fullmatch(r"[0-9a-f]{40}", args.driver_commit):
    parser.error("driver commit must be a full SHA")
jar = args.jar.resolve()
if not jar.is_file():
    parser.error("packaged JDBC jar is required")
root = Path(__file__).resolve().parent.parent
output = args.output.resolve()
output.mkdir(parents=True, exist_ok=True)
java_home = os.environ.get("JAVA_HOME")
java = str(Path(java_home) / "bin/java") if java_home else shutil.which("java")
javac = str(Path(java_home) / "bin/javac") if java_home else shutil.which("javac")
jstack = str(Path(java_home) / "bin/jstack") if java_home else shutil.which("jstack")
if not java or not javac:
    parser.error("a JDK is required")
cases = [
    ("parser-control", 16, 1, 1),
    ("statement-comment", 65536, 1, 3),
    ("statement-comment", 1048576, 8, 3),
    ("statement-tokens", 65536, 8, 3),
    ("prepared-tokens", 262144, 8, 3),
    ("prepared-many", 8192, 8, 2),
    ("literal-open", 1048576, 8, 3),
    ("literal-closed", 1048576, 8, 3),
    ("batch-invalid", 8192, 8, 3),
]
if args.profile == "baseline":
    cases += [(name, size, workers, 5)
              for name in ["statement-comment", "literal-open"]
              for size in [16384, 262144, 1048576]
              for workers in [1, 16]]
result = {
    "schema_version": 1,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
    "driver_source_commit": args.driver_commit,
    "profile": args.profile,
    "heap": "256m", "stack": "1m", "case_timeout_seconds": args.case_timeout,
    "scope": "actual JDBC calls against loopback Emulator; synthetic credentials; no cloud service",
    "cases": [],
}
try:
    result["harness_source_commit"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL).strip()
    result["harness_worktree_dirty"] = bool(subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=root, text=True))
except subprocess.CalledProcessError:
    result["harness_source_commit"] = None
    result["harness_worktree_dirty"] = None
for name in ["source", "image"]:
    identity = root / "target" / ("emulator-" + name + ".txt")
    result["emulator_" + name] = identity.read_text().strip() if identity.exists() else None
runtime = root / "target/emulator-runtime.json"
result["emulator_runtime"] = json.loads(runtime.read_text()) if runtime.exists() else None
with tempfile.TemporaryDirectory(prefix="jdbc-sql-perf-") as classes:
    subprocess.run([javac, "-source", "8", "-target", "8", "-d", classes,
                    str(root / "scripts/JdbcSqlPerformance.java")], check=True)
    for name, size, workers, iterations in cases:
        key = f"{name}-{size}-{workers}"
        log_file = output / (key + ".log")
        command = [java, "-Xmx256m", "-Xss1m", "-XX:+ExitOnOutOfMemoryError", "-verbose:class",
                   "-Dlogback.configurationFile=" + str(root / "scripts/ci-logging.xml"),
                   "-cp", classes + os.pathsep + str(jar), "JdbcSqlPerformance", args.endpoint,
                   name, str(size), str(workers), str(iterations)]
        start = time.monotonic()
        timed_out = False
        with log_file.open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
            try:
                returncode = process.wait(timeout=args.case_timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                if jstack:
                    try:
                        with (output / (key + "-threads.txt")).open("w") as dump:
                            subprocess.run([jstack, "-l", str(process.pid)], stdout=dump,
                                           stderr=subprocess.STDOUT, timeout=8)
                    except (OSError, subprocess.TimeoutExpired):
                        pass
                process.kill()
                returncode = process.wait()
        text = log_file.read_text(errors="replace")
        full_parser = bool(re.search(r"(?:Loaded |class,load\]\s+)com\.aliyun\.odps\.sqa\.commandapi\.antlr\.sql\.OdpsParser(?:\s|\$)", text))
        metrics = []
        failures = []
        for line in text.splitlines():
            if line.startswith("JDBC_PERF_RESULT "):
                try:
                    metrics.append(json.loads(line.split("JDBC_PERF_RESULT ", 1)[1]))
                except json.JSONDecodeError:
                    failures.append("malformed metrics record")
        if timed_out:
            failures.append("hard JVM deadline exceeded")
        if returncode != 0:
            failures.append("JVM failed, exit=" + str(returncode))
        if len(metrics) != 1:
            failures.append("expected exactly one completed metrics record")
        if full_parser != (name == "parser-control"):
            failures.append("full ANTLR parser loading differs from default/control contract")
        if metrics:
            m = metrics[0]
            if m["operations"] != workers * iterations:
                failures.append("incomplete operations")
            if m["p99_ms"] > 20000:
                failures.append("p99 exceeded generous 20 second stability budget")
        row = {"workload": name, "requested_chars": size, "workers": workers,
               "iterations": iterations, "returncode": returncode, "timed_out": timed_out,
               "process_wall_seconds": round(time.monotonic() - start, 3),
               "full_antlr_parser_loaded": full_parser,
               "placeholder_lexer_loaded": "com.aliyun.odps.sqa.commandapi.antlr.sql.OdpsLexer " in text,
               "passed": not failures, "failures": failures, "metrics": metrics[0] if metrics else None}
        result["cases"].append(row)
        print(json.dumps({"case": key, "passed": row["passed"], "failures": failures,
                          "p95_ms": metrics[0]["p95_ms"] if metrics else None}), flush=True)
        (output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
result["finished_at"] = datetime.now(timezone.utc).isoformat()
result["passed"] = all(row["passed"] for row in result["cases"])
(output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
sys.exit(0 if result["passed"] else 1)
