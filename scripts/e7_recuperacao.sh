#!/usr/bin/env bash
# E7 - Recuperação.
#  Parte 1 (5x): isola o líder, grava 1000 chaves pela maioria, reconecta e mede o tempo até o
#                antigo líder voltar como seguidor (termo novo) e alcançar o RAFT APPLIED INDEX.
#  Parte 2:      membro parado enquanto o cluster recebe 12000 escritas; com SNAPSHOT_COUNT=1000
#                ele se recupera por SNAPSHOT; com o padrão (100000) recupera reaplicando o log.
# Uso: ./scripts/e7_recuperacao.sh [1|2]   (sem argumento roda as duas partes)
. "$(dirname "$0")/lib.sh"
registrar e7_recuperacao
PARTE=${1:-todas}
REPS=${REPS:-5}
TMP=$(mktemp)
CSV=resultados/e7_recuperacao.csv
[ -f "$CSV" ] || echo "cenario,repeticao,membro,entradas_atras,reingresso_s,sincronizou_apos_reingresso_ms" > "$CSV"
trap 'restaurar; rm -f "$TMP"' EXIT

json() { docker exec -i cliente python -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
eps_sem() { for m in $MEMBROS; do [ "$m" != "$1" ] && printf '%s,' "$(ep_cliente "$m")"; done | sed 's/,$//'; }

if [ "$PARTE" != 2 ]; then
  etapa "Parte 1: reconexão de um líder isolado ($REPS repetições, 1000 escritas durante o isolamento)"
  for i in $(seq 1 "$REPS"); do
    L=$(lider)
    sonda_bg "$TMP" eleicao 30; esperar_pronto "$TMP"
    docker network disconnect "$REDE_INTERNA" "$L"
    wait "$SONDA_PID"
    ANTES=$(sonda status | awk -v m="$L" '$1==m{print $7}')
    docker exec -e ETCD_ENDPOINTS="$(eps_sem "$L")" cliente python /scripts/carga.py 1000 4 /e7/ >/dev/null
    sonda_bg "$TMP" sincronizar "$L" 60; esperar_pronto "$TMP"
    docker network connect --ip "$(ip_peer "$L")" "$REDE_INTERNA" "$L"
    wait "$SONDA_PID"
    J=$(grep '^{' "$TMP")
    echo "  rep $i: $J"
    echo "$J" | json "','.join(map(str,['reconexao',$i,d['membro'],d['aplicado_lider']-$ANTES,d['reingresso_s'],d['sincronizou_apos_reingresso_ms']]))" >> "$CSV"
    sleep 2
  done
  echo
  printf "tempo de catch-up após reingresso (ms): "
  grep '^reconexao,' "$CSV" | tail -n "$REPS" | cut -d, -f6 | estatistica
  echo ">> estado final idêntico nas réplicas (mesmo hash do keyspace na mesma revisão):"
  ctl_todos etcd1 endpoint hashkv -w table
  ctl_todos etcd1 endpoint status -w table
fi

recuperar_parado() {  # $1 = rótulo do cenário
  local L F
  L=$(lider)
  F=$(for m in $MEMBROS; do [ "$m" != "$L" ] && echo "$m"; done | tail -1)
  echo "líder: $L   membro que ficará parado: $F"
  docker stop "$F" >/dev/null
  local ANTES; ANTES=$(sonda status | awk -v m="$L" '$1==m{print $7}')
  # linhas de log já existentes (para mostrar só o que aconteceu depois daqui)
  local NL NF; NL=$(docker logs "$L" 2>&1 | wc -l); NF=$(docker logs "$F" 2>&1 | wc -l)
  docker exec -e ETCD_ENDPOINTS="$(eps_sem "$F")" cliente python /scripts/carga.py 12000 8 /e7snap/
  sonda_bg "$TMP" sincronizar "$F" 120; esperar_pronto "$TMP"
  docker start "$F" >/dev/null
  wait "$SONDA_PID"
  local J; J=$(grep '^{' "$TMP")
  echo "resultado: $J"
  echo "$J" | json "','.join(map(str,['$1',1,d['membro'],d['aplicado_lider']-$ANTES,d['reingresso_s'],d['sincronizou_apos_reingresso_ms']]))" >> "$CSV"
  echo ">> mensagens de snapshot no log do LÍDER ($L) desde a parada:"
  docker logs "$L" 2>&1 | tail -n +"$((NL + 1))" | grep -io '"msg":"[^"]*snapshot[^"]*"' | sort | uniq -c || echo "   (nenhuma)"
  echo ">> mensagens de snapshot no log do membro que voltou ($F):"
  docker logs "$F" 2>&1 | tail -n +"$((NF + 1))" | grep -io '"msg":"[^"]*snapshot[^"]*"' | sort | uniq -c || echo "   (nenhuma)"
  echo ">> linha do tempo do membro que voltou (carimbos do próprio log do etcd):"
  docker logs "$F" 2>&1 | tail -n +"$((NF + 1))" | docker exec -i cliente python /scripts/marcos_log.py
  ctl_todos etcd1 endpoint status -w table
}

if [ "$PARTE" != 1 ]; then
  etapa "Parte 2a: SNAPSHOT_COUNT=1000 (snapshot a cada 1000 entradas; log antigo é compactado)"
  SNAPSHOT_COUNT=1000 ./scripts/reset.sh 2>&1 | grep -E "saudável|ERRO|Error" || true
  recuperar_parado snapshot_1000
  etapa "Parte 2b: SNAPSHOT_COUNT padrão (100000): o líder ainda tem o log e envia as entradas"
  ./scripts/reset.sh 2>&1 | grep -E "saudável|ERRO|Error" || true
  recuperar_parado log_100000
fi
