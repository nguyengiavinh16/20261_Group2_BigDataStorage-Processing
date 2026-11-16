# infra/cloud/: GCP (phase P4, owner R1)

Intentionally empty until the local MVP works (end of P2/P3). Planned content (see docs/cloud-migration.md):

```
cloud/
├── terraform/            # or gcloud scripts: VPC, firewall, 4 VMs, static IP, GCS bucket, budget alerts
├── kubeadm/              # init/join scripts, Calico, Longhorn (only for Kafka/Mongo/Postgres volumes)
├── overlays/             # cloud versions of k8s manifests (3 Kafka nodes, RF=3, GCS endpoint, nodeSelectors)
└── scripts/              # cluster-up.sh / cluster-down.sh (start/stop VMs to save cost)
```

Do not commit service-account keys or HMAC secrets here.
