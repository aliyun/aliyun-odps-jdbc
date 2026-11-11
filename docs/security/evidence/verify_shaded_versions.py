"""Verify which third-party copies a shaded driver JAR actually carries.

Usage:
  python3 verify_shaded_versions.py --jar PATH [--expect group:artifact=version]
                                    [--bytecheck group:artifact=v1,v2 ...] [--out JSON]

Two independent readings, both taken from the shipped bytes:
  1. META-INF/maven/<group>/<artifact>/pom.properties self-report (assert equals --expect).
  2. Byte-hash scoring of the classes the JAR carries *unrelocated*, against each candidate
     version of the same artifact fetched from Maven Central (--bytecheck). The candidate
     that explains every overlapping class with zero mismatch is the copy that shipped;
     the other candidates are the negative controls.
Diagnostics go to stderr so a caller can capture stdout cleanly. Exit code is nonzero when
an --expect assertion fails, when a --bytecheck has no exact candidate, or when the JAR has
duplicate entry names.
"""
import argparse, collections, hashlib, json, pathlib, re, sys, tempfile, urllib.request, zipfile


def log(msg):
    print(msg, file=sys.stderr)


def coord_url(group, artifact, version):
    return ("https://repo1.maven.org/maven2/%s/%s/%s/%s-%s.jar"
            % (group.replace(".", "/"), artifact, version, artifact, version))


def fetch(url, dest):
    if not dest.exists() or dest.stat().st_size == 0:
        log("fetch %s" % url)
        tmp = dest.with_suffix(dest.suffix + ".part")
        urllib.request.urlretrieve(url, tmp)
        tmp.rename(dest)
    return dest


def class_hashes(jarpath, prefix=None):
    out = {}
    with zipfile.ZipFile(jarpath) as z:
        for n in z.namelist():
            if n.endswith(".class") and (prefix is None or n.startswith(prefix)):
                out[n] = hashlib.sha256(z.read(n)).hexdigest()
    return out


def pom_props_versions(jarpath):
    found = {}
    with zipfile.ZipFile(jarpath) as z:
        for n in z.namelist():
            m = re.match(r"META-INF/maven/(.+?)/(.+?)/pom\.properties$", n)
            if not m:
                continue
            kv = dict(l.split("=", 1) for l in z.read(n).decode("utf-8", "replace").splitlines() if "=" in l)
            found["%s:%s" % (m.group(1), m.group(2))] = kv.get("version")
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jar", required=True)
    ap.add_argument("--expect", action="append", default=[], help="group:artifact=version")
    ap.add_argument("--bytecheck", action="append", default=[], help="group:artifact=v1,v2,v3")
    ap.add_argument("--cache", default="/tmp/shaded-verify-cache")
    ap.add_argument("--out")
    a = ap.parse_args()

    cache = pathlib.Path(a.cache).resolve()
    cache.mkdir(parents=True, exist_ok=True)
    jar = pathlib.Path(a.jar)
    with zipfile.ZipFile(jar) as z:
        names = z.namelist()
    props = pom_props_versions(jar)
    ship = class_hashes(jar)
    dup = [k for k, v in collections.Counter(names).items() if v > 1]
    report = {"jar": str(jar), "jar_bytes": jar.stat().st_size,
              "jar_sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
              "entries": len(names), "classes": len(ship), "duplicate_entry_names": len(dup),
              "expect": {}, "bytecheck": {}}
    failures = []

    for spec in a.expect:
        coord, want = spec.split("=")
        got = props.get(coord)
        ok = got == want
        report["expect"][coord] = {"expected": want, "in_jar_pom_properties": got, "match": ok}
        log("EXPECT %s want=%s got=%s match=%s" % (coord, want, got, ok))
        if not ok:
            failures.append("expect %s: jar self-report says %s, wanted %s" % (coord, got, want))

    for spec in a.bytecheck:
        coord, vspec = spec.split("=")
        group, artifact = coord.split(":")
        for v in vspec.split(","):
            try:
                cand = class_hashes(fetch(coord_url(group, artifact, v), cache / ("%s-%s.jar" % (artifact, v))))
            except Exception as exc:
                log("  candidate %s@%s unusable: %s" % (coord, v, exc))
                report["bytecheck"].setdefault(coord, {})[v] = {"error": str(exc)}
                continue
            overlap = [n for n in ship if n in cand]
            mismatch = [n for n in overlap if ship[n] != cand[n]]
            exact = bool(overlap) and not mismatch
            report["bytecheck"].setdefault(coord, {})[v] = {
                "shipped_classes_in_coord": len(overlap), "mismatch": len(mismatch), "exact": exact}
            log("  bytecheck %s@%s overlap=%d mismatch=%d exact=%s" % (coord, v, len(overlap), len(mismatch), exact))
        if not any(r.get("exact") for r in report["bytecheck"][coord].values()):
            failures.append("bytecheck %s: no candidate explains every shipped class byte-for-byte" % coord)

    if dup:
        failures.append("duplicate entry names: %d e.g. %s" % (len(dup), dup[:3]))

    if a.out:
        pathlib.Path(a.out).write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    for f in failures:
        log("FAIL " + f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
