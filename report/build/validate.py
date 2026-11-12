import json, os, sys, glob
import jsonschema
from jsonschema import RefResolver, Draft7Validator
S = os.path.dirname(os.path.dirname(os.path.abspath(__file__))); SCH = os.path.join(S, 'schemas'); OUT = os.path.join(S, sys.argv[1] if len(sys.argv) > 1 else 'report_new')
BASE = "https://developer.microsoft.com/json-schemas/fabric/item/report/"
store = {}
for f in os.listdir(SCH):
    parts = f[:-5].split('_')
    if parts[-1].startswith('schema'): tail = parts[-1]; parts = parts[:-1]
    else: tail = 'schema'
    if parts[0] not in ('definition', 'definitionProperties'): parts = ['definition'] + parts
    url = BASE + "/".join(parts) + "/" + tail + ".json"; store[url] = json.load(open(os.path.join(SCH, f)))
def check(path):
    doc = json.load(open(path)); url = doc.get("$schema")
    if url not in store: return [f"no local schema for {url}"]
    v = Draft7Validator(store[url], resolver=RefResolver(url, store[url], store=store))
    return [f"{'/'.join(str(x) for x in e.absolute_path)}: {e.message[:200]}" for e in v.iter_errors(doc)]
bad = 0; n = 0
for path in sorted(glob.glob(os.path.join(OUT, '**', '*.json'), recursive=True)):
    if 'StaticResources' in path: continue
    n += 1; errs = check(path)
    if errs:
        bad += 1; print("FAIL", os.path.relpath(path, OUT))
        for e in errs[:6]: print("     ", e)
print(f"validated {n} files, {bad} with errors")
