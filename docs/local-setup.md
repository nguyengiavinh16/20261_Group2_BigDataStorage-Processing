# Local setup (every team member)

Target: any laptop with **16 GB RAM** (32 GB comfortable), 60 GB free disk. Windows users work **inside WSL2 Ubuntu**
(clone the repo inside the WSL filesystem, e.g. `~/code/cryptoguard`, not under `/mnt/c`: much faster I/O).

## 1. Install

| Tool | Why | Check |
|---|---|---|
| WSL2 + Ubuntu 22.04/24.04 (Windows only) | Linux shell, Makefile, bash scripts | `wsl -l -v` |
| Docker Desktop (WSL2 backend) or Docker Engine | runtime for kind nodes and image builds (we do NOT run services with docker-compose) | `docker info` |
| kind | local Kubernetes | `kind version` |
| kubectl | talk to the cluster | `kubectl version --client` |
| helm 3 | install operators/charts | `helm version` |
| Python 3.11 + venv | code, tests | `python3.11 --version` |
| make, git | entry points | |
| k9s (optional) | terminal UI for the cluster | |
| Java 17 (R2, R3, R4 only) | PyFlink / PySpark local runs | `java -version` |

Docker Desktop: Settings → Resources → give it **≥ 8 GB RAM** (12 GB if you run stream + batch together).

## 2. First run

```bash
git clone <repo-url> cryptoguard && cd cryptoguard
cp .env.example .env
make dev-install          # .venv + libs/common + dev tools
make test                 # must be green before anything else
make gen-topics           # only if you changed contracts/kafka-topics.yaml
make up                   # kind + Kafka + MinIO + Postgres (~5–10 min first time)
make status
```

Then only the profiles you need: `make up-stream` (R2, R3), `make up-batch` (R4, R5), `make up-serving` (R3, R5),
`make up-monitoring` (R1).

## 3. Smoke tests

```bash
make seed-samples                                        # sample messages into Kafka
kubectl -n cg-kafka get kafkatopics                      # 14 topics Ready
curl -s http://localhost:30900/minio/health/ready -o /dev/null -w '%{http_code}\n'   # 200
```

## 4. Troubleshooting

| Symptom | Fix |
|---|---|
| Kafka client hangs from host | use `localhost:32100` (NodePort mapped by kind); the broker advertises `localhost:32000` |
| Pods `Pending` | not enough RAM in Docker Desktop; stop other profiles (`kubectl scale ... --replicas=0`) |
| `ImagePullBackOff` for `cg-*` images | build + load: `make image-<component>` (`imagePullPolicy: IfNotPresent/Never`) |
| Binance returns HTTP 451 / connection refused | geo-restriction (US IPs). Locally in Vietnam it works; on cloud pick an Asian region |
| Everything broken | `make down && make up` (local data in PVCs is lost; Bronze can be re-downloaded) |
