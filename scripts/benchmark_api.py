"""Simple API load probe; API_URL should point to a running service."""
import argparse, json, time, statistics
import requests
p=argparse.ArgumentParser(); p.add_argument('--url',default='http://localhost:8000/health'); p.add_argument('--requests',type=int,default=100); a=p.parse_args()
lat=[]; errors=0
for _ in range(a.requests):
 t=time.perf_counter()
 try: r=requests.get(a.url,timeout=5); r.raise_for_status()
 except Exception: errors+=1
 lat.append((time.perf_counter()-t)*1000)
lat.sort(); result={'requests':a.requests,'errors':errors,'error_rate':errors/a.requests,'mean_latency_ms':statistics.mean(lat),'p50_ms':lat[int(.50*(len(lat)-1))],'p95_ms':lat[int(.95*(len(lat)-1))]}
print(json.dumps(result,indent=2))
