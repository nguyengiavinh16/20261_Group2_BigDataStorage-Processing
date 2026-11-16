# ADR-0001: Local-first Kubernetes (kind), then GCP VMs

- Status: Accepted
- Date: 2026-11-16
- Deciders: whole team

## Context
The course requires Kubernetes on virtual machines instead of docker-compose on a laptop. Cloud credits are limited
(~$300) and burn while VMs run. Most of the work (writing jobs, debugging contracts) does not need a real cluster.

## Decision
- P0–P3: each member runs a **single-node kind cluster** on their laptop with the same operators (Strimzi, Flink
  Operator, Spark Operator), manifests and images as production. Profiles (`make up-*`) keep RAM usage low.
- P4: move to **GCP Compute Engine VMs** with **kubeadm**. Same manifests + cloud overlays; `CG_PROFILE=cloud`.
- Docker is only used to build images and as the kind node runtime: no service runs via docker-compose.

## Consequences
- + Cheap, fast iteration; cloud time is spent only on scale tests and the final demo.
- + "Works on kind" is close to "works on kubeadm": the same K8s APIs.
- − Laptops cannot run everything at once; integration tests across all components wait for P4 or a 32 GB machine.
- Follow-up: INF-03 pins tool versions; INF-20+ cloud tasks.

## Alternatives considered
- GKE Standard (less to operate, but less "VM + Kubernetes" learning; still an option if kubeadm blocks P4).
- minikube/k3d locally (equivalent; kind chosen for multi-node option and wide CI use).
