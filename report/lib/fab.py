import json, sys, subprocess, urllib.request, time, base64, os
def tok(res="https://api.fabric.microsoft.com"):
    return subprocess.run(['az','account','get-access-token','--resource',res,'--query','accessToken','-o','tsv'],capture_output=True,text=True).stdout.strip()
def call(method, url, body=None, res="https://api.fabric.microsoft.com"):
    req=urllib.request.Request(url, method=method, data=(json.dumps(body).encode() if body is not None else None), headers={"Authorization":"Bearer "+tok(res),"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=300) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
def lro(method, url, body=None):
    code, h, data = call(method, url, body)
    if code == 202:
        loc = h.get('Location') or h.get('location')
        for _ in range(120):
            time.sleep(int(h.get('Retry-After', 2)) if _==0 else 3)
            c2, h2, d2 = call('GET', loc)
            st = json.loads(d2 or b'{}')
            if st.get('status') in ('Succeeded','Failed'):
                if st['status']=='Failed': return 500, st
                c3, h3, d3 = call('GET', loc + '/result')
                return c3, (json.loads(d3) if d3 else {})
        return 504, {}
    return code, (json.loads(data) if data else {})
