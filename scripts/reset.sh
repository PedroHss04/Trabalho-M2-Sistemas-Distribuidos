#!/usr/bin/env bash
# Recria o laboratório do zero (3 membros, sem etcd4) e grava a configuração inicial.
# Use antes de cada repetição do e9_reconfig.sh.
#   SNAPSHOT_COUNT=1000 ./scripts/reset.sh   -> snapshots frequentes (E7 parte 2)
. "$(dirname "$0")/lib.sh"
docker compose --profile extra down --remove-orphans
# --wait: só retorna quando os 3 membros passam no healthcheck (endpoint health, exige quórum)
docker compose up -d --wait
sonda esperar-saudavel 60
ctl etcd1 put /config/modo '{"valor": "producao"}' >/dev/null
ctl etcd1 put /config/log_level info >/dev/null
./scripts/status.sh
