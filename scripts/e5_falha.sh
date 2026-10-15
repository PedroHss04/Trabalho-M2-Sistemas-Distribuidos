#!/usr/bin/env bash
# E5 - Falha e quórum.
#  1. Derruba o líder 5x com docker kill (queda abrupta) e 5x com docker stop (desligamento
#     gracioso) e mede a janela sem líder com a sonda (mesmo relógio, amostragem ~10 ms).
#  2. Quadro: membros ativos x leitura linearizável x leitura serializável x escrita.
. "$(dirname "$0")/lib.sh"
registrar e5_falha
trap restaurar EXIT
REPS=${REPS:-5}
CSV=resultados/e5_eleicao.csv
[ -f "$CSV" ] || echo "modo,repeticao,lider_antigo,termo_antigo,lider_novo,termo_novo,sem_lider_ms" > "$CSV"
TMP=$(mktemp)

derrubar_lider() {  # $1 = kill | stop ; $2 = repetição
  sonda_bg "$TMP" eleicao 30
  esperar_pronto "$TMP"
  local L; L=$(sed -n 's/^pronto: líder atual \(etcd[0-9]\).*/\1/p' "$TMP")
  docker "$1" "$L" >/dev/null
  wait "$SONDA_PID"
  local j; j=$(grep '^{' "$TMP")
  echo "  [$1 #$2] $j"
  echo "$j" | docker exec -i cliente python -c "import json,sys; d=json.load(sys.stdin); \
print(','.join(map(str,['$1',$2,d['lider_antigo'],d['termo_antigo'],d['lider_novo'],d['termo_novo'],d['sem_lider_ms']])))" >> "$CSV"
  docker start "$L" >/dev/null
  sonda esperar-saudavel 60 >/dev/null
  sleep 2
}

etapa "1a. docker kill no líder (queda abrupta): os outros esperam o election timeout"
for i in $(seq 1 "$REPS"); do derrubar_lider kill "$i"; done
etapa "1b. docker stop no líder (SIGTERM): o etcd transfere a liderança antes de sair"
for i in $(seq 1 "$REPS"); do derrubar_lider stop "$i"; done

etapa "Resumo da janela sem líder (ms)"
for m in kill stop; do
  printf "%-5s " "$m"; grep "^$m," "$CSV" | tail -n "$REPS" | cut -d, -f7 | estatistica
done
echo "termos ao longo das falhas: $(cut -d, -f4,6 "$CSV" | tail -n +2 | tr '\n' ' ')"

etapa "2. Quadro membros ativos x operações (endpoint único do sobrevivente, timeout 3 s)"
T="--command-timeout=3s"
ok() { if out=$("$@" 2>&1); then echo "OK"; else echo "FALHA ($(echo "$out" | tail -1 | cut -c1-60))"; fi; }
linha() {  # $1 = membro consultado
  local m=$1
  printf "| %-32s | %-7s | %-10s | %-10s |\n" "$2" \
    "$(ok ctl "$m" $T put /e5/x v | cut -d" " -f1)" \
    "$(ok ctl "$m" $T get /e5/x | cut -d" " -f1)" \
    "$(ok ctl "$m" $T get /e5/x --consistency=s | cut -d" " -f1)"
}
L=$(lider)
OUTROS=$(for m in $MEMBROS; do [ "$m" != "$L" ] && echo "$m"; done)
A=$(echo "$OUTROS" | head -1); B=$(echo "$OUTROS" | tail -1)
echo "| membros ativos                   | escrita | leit. lin. | leit. ser. |"
echo "|----------------------------------|---------|------------|------------|"
linha "$L" "3 de 3 ($MEMBROS)"
docker stop "$A" >/dev/null; sleep 3
linha "$L" "2 de 3 (parado: $A)"
docker stop "$B" >/dev/null; sleep 3
linha "$L" "1 de 3 (parados: $A, $B)"
echo
echo ">> mensagens completas no estado sem maioria (consultando $L):"
echo "put:          $(ctl "$L" $T put /e5/x v 2>&1 | tail -1)"
echo "get lin.:     $(ctl "$L" $T get /e5/x 2>&1 | tail -1)"
echo "get serial.:  $(ctl "$L" $T get /e5/x --consistency=s --print-value-only 2>&1 | tail -1)"
echo ">> status do sobrevivente (sem quórum ele deixa de ser líder):"
ctl "$L" endpoint status -w table || true
rm -f "$TMP"
