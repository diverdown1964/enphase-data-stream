import sys, os, json, base64, glob
S = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); sys.path.insert(0, os.path.join(S, 'lib'))
from fab import lro, call
import pbi_export
WS = "d31aaac9-0ac1-422b-849b-9a34d354eac4"; NAME = "Hale Joli Solar 2.0"; OUT = os.path.join(S, 'report_new'); IDF = os.path.join(S, 'build', 'report_id.txt')
parts = []
for fp in sorted(glob.glob(os.path.join(OUT, '**', '*'), recursive=True)):
    if os.path.isfile(fp):
        parts.append({"path": os.path.relpath(fp, OUT).replace(os.sep, '/'), "payload": base64.b64encode(open(fp, 'rb').read()).decode(), "payloadType": "InlineBase64"})
rid = open(IDF).read().strip() if os.path.exists(IDF) else None
if not rid:
    code, res = lro('POST', f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/reports", {"displayName": NAME, "definition": {"parts": parts}})
    print('create', code, json.dumps(res)[:600])
    rid = res.get('id')
    if not rid:
        c, h, d = call('GET', f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/reports")
        rid = next((r['id'] for r in json.loads(d).get('value', []) if r['displayName'] == NAME), None)
    if not rid: sys.exit('no report id')
    open(IDF, 'w').write(rid)
else:
    code, res = lro('POST', f"https://api.fabric.microsoft.com/v1/workspaces/{WS}/reports/{rid}/updateDefinition", {"definition": {"parts": parts}})
    print('update', code, json.dumps(res)[:600])
    if code not in (200, 201, 202) and res.get('errorCode') != 'OperationHasNoResult': sys.exit(1)
print('report id', rid)
if '--no-export' not in sys.argv:
    exp = os.path.join(S, 'export_new')
    for f in glob.glob(os.path.join(exp, '*.png')): os.remove(f)
    pbi_export.export(WS, rid, exp)
