#!/usr/bin/env bash
# Recria o laboratório do zero (3 membros, sem etcd4) e grava a configuração inicial.
# Use antes de cada repetição do e9_reconfig.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose --profile extra down --remove-orphans
docker compose up -d
echo "aguardando o cluster ficar saudável..."
for i in $(seq 1 60); do
  if docker exec etcd1 etcdctl --endpoints=http://172.31.0.31:2379,http://172.31.0.32:2379,http://172.31.0.33:2379 \
       endpoint health >/dev/null 2>&1; then break; fi
  sleep 1
done
docker exec etcd1 etcdctl put /config/modo '{"valor": "producao"}' >/dev/null
docker exec etcd1 etcdctl put /config/log_level info >/dev/null
./scripts/status.sh
