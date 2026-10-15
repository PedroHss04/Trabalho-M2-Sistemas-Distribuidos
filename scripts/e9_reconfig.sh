#!/usr/bin/env bash
# E9 (parte 2) - Ênfase C: reconfiguração de membros sem perda de disponibilidade.
#
#   1. adiciona etcd4 como LEARNER (recebe o log, não vota)
#   2. sobe o etcd4 e mede o tempo até ele alcançar o líder (catch-up)
#   3. promove o etcd4 a votante (quórum passa de 2/3 para 3/4)
#   4. remove outro membro (padrão: etcd3) e volta a ter 3 votantes
#
# Uso (na pasta do projeto, com o cluster no ar):
#   ./scripts/e9_reconfig.sh               # pausa entre as etapas (bom para gravar o vídeo)
#   AUTO=1 ./scripts/e9_reconfig.sh        # sem pausas (bom para repetir 5x e medir)
#   REMOVER=etcd1 ./scripts/e9_reconfig.sh # escolhe qual membro remover no fim
#
# Dica: em outro terminal deixe rodando
#   docker exec cliente python /scripts/escritor.py 180
# para provar que as escritas continuam durante toda a reconfiguração.
set -euo pipefail
cd "$(dirname "$0")/.."

REMOVER=${REMOVER:-etcd3}
NOVO=etcd4
NOVO_PEER=http://172.30.0.34:2380
NOVO_CLIENTE=172.31.0.34:2379
mkdir -p resultados
LOG="resultados/reconfig_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG") 2>&1

# executa o etcdctl num membro que não será removido
CTL_NO=etcd1; [ "$REMOVER" = etcd1 ] && CTL_NO=etcd2
ctl() { docker exec "$CTL_NO" etcdctl --endpoints=http://127.0.0.1:2379 "$@"; }
agora() { date +%s.%N; }
dif() { awk -v a="$1" -v b="$2" 'BEGIN{printf "%.2f", b-a}'; }
etapa() { echo; echo "=================== $* ==================="; }
pausa() { [ "${AUTO:-0}" = 1 ] || read -rp ">> ENTER para continuar..." _; }
id_de() { ctl member list | grep ", $1, " | cut -d, -f1; }
status() {
  ctl member list -w table
  ctl endpoint status --cluster -w table 2>/dev/null || true
}
indice_raft() {  # RAFT APPLIED INDEX de um endpoint (9ª coluna da saída simples)
  docker exec "$CTL_NO" etcdctl --endpoints="$1" endpoint status 2>/dev/null | awk -F', ' '{print $9}'
}

T_INICIO=$(agora)

etapa "0. Estado inicial"
status
pausa

etapa "1. member add $NOVO --learner"
T=$(agora)
ctl member add "$NOVO" --learner --peer-urls="$NOVO_PEER"
echo ">> learner registrado em $(dif "$T" "$(agora)") s. Votantes continuam 3 => quórum = 2."
ctl member list -w table
pausa

etapa "2. Subindo o $NOVO e medindo o catch-up (learner alcançando o líder)"
T=$(agora)
docker compose --profile extra up -d "$NOVO"
for i in $(seq 1 300); do
  LIDER=$(indice_raft http://127.0.0.1:2379 || true)
  APRENDIZ=$(indice_raft "$NOVO_CLIENTE" || true)
  if [ -n "$APRENDIZ" ] && [ -n "$LIDER" ] && [ "$APRENDIZ" -ge "$((LIDER - 1))" ]; then break; fi
  sleep 0.2
done
T_CATCHUP=$(dif "$T" "$(agora)")
echo ">> RAFT INDEX: referência=$LIDER  $NOVO=$APRENDIZ  -> catch-up em $T_CATCHUP s"
echo ">> O learner NÃO atende leitura linearizável (esperado: erro 'rpc not supported for learner'):"
docker exec "$CTL_NO" etcdctl --endpoints="$NOVO_CLIENTE" get /config/modo 2>&1 | tail -1 || true
echo ">> ...mas atende leitura serializável (dado local):"
docker exec "$CTL_NO" etcdctl --endpoints="$NOVO_CLIENTE" get /config/modo --consistency=s 2>&1 | tail -1 || true
pausa

etapa "3. member promote $NOVO (learner -> votante)"
ID_NOVO=$(id_de "$NOVO")
T=$(agora)
until ctl member promote "$ID_NOVO" 2>/dev/null; do
  echo "   ainda não sincronizado o suficiente para promover; tentando de novo..."; sleep 0.5
done
T_PROMOTE=$(dif "$T" "$(agora)")
echo ">> promovido em $T_PROMOTE s. Agora 4 votantes => quórum = 3 (tolera só 1 falha, igual a 3 membros!)"
status
pausa

etapa "4. member remove $REMOVER"
ID_REM=$(id_de "$REMOVER")
T=$(agora)
ctl member remove "$ID_REM"
T_REMOVE=$(dif "$T" "$(agora)")
docker stop "$REMOVER" >/dev/null
echo ">> removido em $T_REMOVE s e contêiner parado. Voltamos a 3 votantes => quórum = 2."
status

etapa "Resumo"
echo "catch-up do learner : $T_CATCHUP s"
echo "promote             : $T_PROMOTE s"
echo "remove              : $T_REMOVE s"
echo "total               : $(dif "$T_INICIO" "$(agora)") s"
echo "log salvo em $LOG"
echo "CSV;$T_CATCHUP;$T_PROMOTE;$T_REMOVE" >> resultados/reconfig_tempos.csv
