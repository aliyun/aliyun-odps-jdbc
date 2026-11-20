"""Scan resolved Maven text trees against OSV; inputs must contain public coordinates only.
Usage: python3 scan_maven_trees.py OUTPUT_JSON TREE [TREE ...]
Includes test/provided findings separately; does not infer exploitability or inspect shaded classes.
"""
import concurrent.futures
import datetime
import json
import pathlib
import re
import sys
import urllib.request

def fetch(url, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)

output, *files = sys.argv[1:]
packages = {}
for file in files:
    for line in pathlib.Path(file).read_text().splitlines()[1:]:
        match = re.search(r'([\w.\-]+):([\w.\-]+):jar:(?:[\w_\-]+:)?([^: ]+):(compile|runtime|test|provided)', line)
        if not match:
            continue
        group, artifact, version, scope = match.groups()
        # Do not send product/private coordinates to an external service.
        if group.startswith(('com.aliyun', 'com.alibaba', 'apsara')):
            continue
        key = (group + ':' + artifact, version)
        packages.setdefault(key, set()).add(scope)
queries = [{'package': {'ecosystem': 'Maven', 'name': n}, 'version': v} for n, v in sorted(packages)]
results = []
for start in range(0, len(queries), 100):
    batch = fetch('https://api.osv.dev/v1/querybatch', {'queries': queries[start:start + 100]})['results']
    if len(batch) != len(queries[start:start + 100]):
        raise RuntimeError('Incomplete OSV batch')
    if any(item.get('next_page_token') for item in batch):
        raise RuntimeError('OSV pagination required; refusing incomplete scan')
    results.extend(batch)
ids = sorted({v['id'] for r in results for v in r.get('vulns', [])})
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
    advisories = dict(zip(ids, pool.map(lambda i: fetch('https://api.osv.dev/v1/vulns/' + i), ids)))
rows = []
for query, result in zip(queries, results):
    name, version = query['package']['name'], query['version']
    rows.append({'name': name, 'version': version, 'scopes': sorted(packages[(name, version)]),
                 'advisories': [v['id'] for v in result.get('vulns', [])]})
pathlib.Path(output).write_text(json.dumps({'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'source': 'https://api.osv.dev/v1/querybatch', 'complete': True, 'packages': rows, 'advisories': advisories}, indent=2))
print('packages', len(rows), 'flagged', sum(bool(r['advisories']) for r in rows), 'unique_advisories', len(ids))
for row in rows:
    if row['advisories']:
        print(row['name'], row['version'], ','.join(row['scopes']), ','.join(row['advisories']))
