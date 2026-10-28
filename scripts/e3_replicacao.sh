#!/usr/bin/env bash
# E3 - Replicação: escreve num membro, lê noutro, watch num terceiro; mede a propagação
# (20 repetições) e roda o check perf como referência de desempenho.
. "$(dirname "$0")/lib.sh"
registrar e3_replicacao
trap 'limpar_avulsos e3_watch' EXIT

etapa "watch em etcd3 (contêiner avulso), put em etcd1, get em etcd2"
avulso e3_watch watch /e3/x -w json --endpoints=http://172.31.0.33:2379
sleep 1.5
ctl etcd1 put /e3/x "escrito-no-etcd1-$(date +%H:%M:%S)"
echo ">> get no etcd2 (endpoint único):"
ctl etcd2 get /e3/x
sleep 1
echo ">> notificação recebida pelo watch aberto no etcd3:"
docker logs e3_watch

etapa "Tempo de propagação: put no líder -> valor visível (leitura local) em cada seguidor"
sonda propagacao 20

etapa "check perf --load=s (referência de desempenho; ~60 s) + memória durante a carga"
( sleep 30; echo; echo ">> memória no meio do check perf:"; \
  docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' etcd1 etcd2 etcd3 ) &
STATS_PID=$!
ctl_todos etcd1 check perf --load=s 2>&1 | sed -E 's/ *[0-9]+ \/ 60 +[0-9.]+%( [0-9]+m[0-9]+s)?//g' | tr -s '\r' '\n' \
  || echo "(check perf reprovou algum critério: ver análise no relatório)"
wait "$STATS_PID"
