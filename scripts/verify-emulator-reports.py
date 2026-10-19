import pathlib,xml.etree.ElementTree as E
names=['MetadataNamespaceContractTest','OdpsScrollResultSetTest','MetadataAndOptionsTest','OdpsStatementCommandApiTest']
counts={k:0 for k in ['tests','failures','errors','skipped']}
for name in names:
 t=E.parse(pathlib.Path('target/surefire-reports')/('TEST-com.aliyun.odps.jdbc.'+name+'.xml')).getroot()
 for k in counts:counts[k]+=int(t.get(k,'0'))
print(counts)
assert counts=={'tests':15,'failures':0,'errors':0,'skipped':0}, 'All 15 namespace contracts must execute and pass'
