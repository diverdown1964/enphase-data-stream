import json, sys, time, urllib.request, subprocess, zipfile, io, os
def tok():
    return subprocess.run(['az','account','get-access-token','--resource','https://analysis.windows.net/powerbi/api','--query','accessToken','-o','tsv'],capture_output=True,text=True).stdout.strip()
def call(method,url,body=None):
    req=urllib.request.Request(url,method=method,data=(json.dumps(body).encode() if body is not None else None),headers={"Authorization":"Bearer "+tok(),"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req,timeout=300) as r: return r.status, r.read()
    except urllib.error.HTTPError as e: return e.code, e.read()
def export(ws, rid, outdir, fmt="PNG"):
    base=f"https://api.powerbi.com/v1.0/myorg/groups/{ws}/reports/{rid}"
    c,b=call('POST',base+'/ExportTo',{"format":fmt})
    if c not in (200,202): print('export start failed',c,b[:500]); return False
    eid=json.loads(b)['id']
    for i in range(90):
        time.sleep(5); c,b=call('GET',f"{base}/exports/{eid}"); st=json.loads(b)
        if st.get('status') in ('Succeeded','Failed'): break
    print('export status', st.get('status'), st.get('percentComplete'), (st.get('error') or ''))
    if st.get('status')!='Succeeded': return False
    c,b=call('GET',f"{base}/exports/{eid}/file")
    os.makedirs(outdir,exist_ok=True)
    if b[:2]==b'PK':
        z=zipfile.ZipFile(io.BytesIO(b)); 
        for n in z.namelist():
            safe=os.path.basename(n); open(os.path.join(outdir,safe),'wb').write(z.read(n)); print('  ',safe,len(z.read(n)))
    else:
        ext='.png' if fmt=='PNG' else '.pdf'; open(os.path.join(outdir,'export'+ext),'wb').write(b); print('  export'+ext,len(b))
    return True
if __name__=='__main__': export(sys.argv[1],sys.argv[2],sys.argv[3])
