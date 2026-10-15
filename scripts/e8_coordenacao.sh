#!/usr/bin/env bash
# E8 - Coordenação e ordem.
#  1. Termo (época de liderança) x revisão (ordem total das escritas) ao longo de uma falha.
#  2. Exclusão mútua com "etcdctl lock" e eleição com "etcdctl elect": dois clientes competem;
#     o detentor é morto (docker kill = crash, sem liberar) e mede-se quanto o outro espera
#     (expiração do lease). Também docker stop (SIGTERM), em que o etcdctl libera na saída.
# Os clientes rodam em contêineres avulsos: matar o cliente não derruba nenhum membro.
. "$(dirname "$0")/lib.sh"
registrar e8_coordenacao
REPS=${REPS:-5}
CSV=resultados/e8_failover.csv
[ -f "$CSV" ] || echo "recurso,modo,repeticao,espera_s" > "$CSV"
trap 'limpar_avulsos e8_A e8_B e8_obs; restaurar' EXIT

etapa "1. Termo x revisão (relógios lógicos) ao longo de uma troca de líder"
cabecalho() { ctl_todos "$(algum_vivo)" put /e8/relogio "$1" -w json | docker exec -i cliente python -c \
  "import json,sys; h=json.load(sys.stdin)['header']; print(f\"$1: revision={h['revision']}  raft_term={h['raft_term']}\")"; }
cabecalho escrita-1
cabecalho escrita-2
L=$(lider); echo ">> derrubando o líder $L (docker kill)"
docker kill "$L" >/dev/null
sonda esperar-saudavel 30
cabecalho escrita-3
cabecalho escrita-4
docker start "$L" >/dev/null; sonda esperar-saudavel 60 >/dev/null
echo ">> a revisão continua crescendo sem saltos estranhos; o termo só muda quando muda o líder."
echo ">> MVCC: a revisão também versiona os dados (leitura de uma revisão passada):"
R=$(ctl etcd1 get /e8/relogio -w json | docker exec -i cliente python -c "import json,sys; print(json.load(sys.stdin)['kvs'][0]['mod_revision'])")
echo "   get /e8/relogio (atual, mod_revision=$R): $(ctl etcd1 get /e8/relogio --print-value-only)"
echo "   get /e8/relogio --rev=$((R - 1)):            $(ctl etcd1 get /e8/relogio --rev=$((R - 1)) --print-value-only)"

competir() {  # $1 = lock | elect ; $2 = kill | stop ; $3 = repetição
  local args_a args_b
  if [ "$1" = lock ]; then args_a=(lock --ttl=10 trava); args_b=(lock --ttl=10 trava)
  else args_a=(elect eleicao candidatoA); args_b=(elect eleicao candidatoB); fi
  avulso e8_A "${args_a[@]}"
  until [ -n "$(docker logs e8_A 2>/dev/null)" ]; do sleep 0.1; done
  avulso e8_B "${args_b[@]}"
  sleep 1.5
  if [ "$3" = 1 ]; then
    echo "   A detém: $(docker logs e8_A 2>&1 | tr '\n' ' ')"
    echo "   B (esperando) imprimiu: '$(docker logs e8_B 2>&1 | tr '\n' ' ')'"
    local pref; [ "$1" = lock ] && pref=trava || pref=eleicao
    echo "   chaves de espera (ordenadas pela create_revision = fila justa / fencing token):"
    ctl etcd1 get "$pref" --prefix -w json | docker exec -i cliente python -c "
import json,sys,base64
for kv in json.load(sys.stdin).get('kvs',[]):
    print('     ', base64.b64decode(kv['key']).decode(), ' create_revision=', kv['create_revision'], ' lease=', kv.get('lease'))"
  fi
  local t0; t0=$(agora)
  docker "$2" e8_A >/dev/null
  until [ -n "$(docker logs e8_B 2>/dev/null)" ]; do sleep 0.1; done
  local t; t=$(dif "$t0" "$(agora)")
  echo "   [$1/$2 #$3] B assumiu $t s depois do $2 em A: $(docker logs e8_B 2>&1 | tr '\n' ' ')"
  echo "$1,$2,$3,$t" >> "$CSV"
  limpar_avulsos e8_A e8_B
  sleep 1
}

etapa "2a. Exclusão mútua: etcdctl lock --ttl=10 trava"
for i in $(seq 1 "$REPS"); do competir lock kill "$i"; done
competir lock stop 1
etapa "2b. Eleição de líder de aplicação: etcdctl elect eleicao (sessão com TTL padrão de 60 s)"
avulso e8_obs elect -l eleicao   # observador: imprime cada novo líder
for i in $(seq 1 "$REPS"); do competir elect kill "$i"; done
competir elect stop 1
echo ">> observador (elect -l) viu a sequência de líderes:"
docker logs e8_obs 2>&1 | sed 's/^/   /'

etapa "Resumo do failover (s)"
for r in lock elect; do for m in kill stop; do
  printf "%-5s %-4s " "$r" "$m"; grep "^$r,$m," "$CSV" | cut -d, -f4 | estatistica
done; done
