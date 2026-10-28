#!/usr/bin/env bash
# E9 (parte 2) com repetição (R7): N rodadas de reset -> carga -> escritor contínuo ->
# reconfiguração completa. Consolida os tempos e a disponibilidade observada pelo escritor.
#   ./scripts/e9_repeticoes.sh              # 5 rodadas removendo o etcd3 (seguidor)
#   REMOVER=lider ./scripts/e9_repeticoes.sh 5   # 5 rodadas removendo o líder
#   REMOVER=lider MOVER_LIDER=1 ./scripts/e9_repeticoes.sh 5   # idem, com move-leader antes
. "$(dirname "$0")/lib.sh"
N=${1:-5}
export REMOVER=${REMOVER:-etcd3} MOVER_LIDER=${MOVER_LIDER:-0}
export CENARIO="remover_${REMOVER}$([ "$MOVER_LIDER" = 1 ] && echo _com_move_leader)"
DISP=resultados/e9_disponibilidade.csv
[ -f "$DISP" ] || echo "data,cenario,escritas,ok,falhas,maior_intervalo_ms,lat_media_ms,lat_max_ms" > "$DISP"

for i in $(seq 1 "$N"); do
  echo "########## rodada $i/$N ($CENARIO) ##########"
  ./scripts/reset.sh 2>&1 | grep -E "saudável|ERRO|Error" || true
  docker exec cliente python /scripts/carga.py 5000 8 | tail -1
  ESC=$(mktemp)
  docker exec cliente python -u /scripts/escritor.py 40 >"$ESC" 2>&1 &
  ESC_PID=$!
  sleep 3
  AUTO=1 ./scripts/e9_reconfig.sh | sed -n '/=== Resumo ===/,$p'
  wait "$ESC_PID"
  R=$(grep '^RESUMO;' "$ESC" | cut -d';' -f2-)
  echo "escritor: $(grep -E '^escritas:|^maior intervalo' "$ESC" | tr '\n' ' ')"
  echo "$(date +%Y%m%d_%H%M%S),$CENARIO,${R//;/,}" >> "$DISP"
  rm -f "$ESC"
done

echo
echo "===== Consolidado do cenário $CENARIO ====="
T=resultados/e9_reconfig_tempos.csv
for col in 5:add_s 6:subida_s 7:catchup_ms 8:promote_s 9:remove_s; do
  printf "%-11s " "${col#*:}"; awk -F, -v c="$CENARIO" -v k="${col%%:*}" '$2==c {print $k}' "$T" | estatistica
done
printf "%-11s " "falhas"; awk -F, -v c="$CENARIO" '$2==c {print $5}' "$DISP" | estatistica
printf "%-11s " "gap_ms"; awk -F, -v c="$CENARIO" '$2==c {print $6}' "$DISP" | estatistica
