#!/usr/bin/env bash
# Snapshot cluster state for a failed run: the first thing anyone triaging asks for.
set -uo pipefail
mkdir -p reports
kubectl get all,endpointslices,ingress,pdb -n inference -o wide > reports/k8s-state.txt
kubectl -n inference describe pods > reports/pods.txt
kubectl -n inference logs -l app=inference --tail=200 --prefix > reports/pod-logs.txt
kubectl -n ingress-nginx logs deploy/ingress-nginx-controller --tail=300 > reports/ingress-logs.txt
kubectl get events -A --sort-by=.lastTimestamp > reports/events.txt
echo "diagnostics in reports/"
