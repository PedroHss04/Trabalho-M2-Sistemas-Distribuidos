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
#   AUTO=1 ./scripts/e9_reconfig.sh        # sem pausas (bom para repetir e medir)
#   REMOVER=lider ./scripts/e9_reconfig.sh # remove o LÍDER atual (força eleição na reconfiguração)
#   REMOVER=etcd2 ./scripts/e9_reconfig.sh # remove um membro específico
#   REMOVER=lider MOVER_LIDER=1 ./scripts/e9_reconfig.sh
#                                          # transfere a liderança (move-leader) ANTES de remover
#
# Dica: em outro terminal deixe rodando
#   docker exec cliente python /scripts/escritor.py 180
# para provar que as escritas continuam durante toda a reconfiguração
# (scripts/e9_repeticoes.sh faz isso automaticamente, 5 vezes).
. "$(dirname "$0")/lib.sh"
registrar reconfig

NOVO=etcd4
NOVO_PEER=http://172.30.0.34:2380
NOVO_CLIENTE=http://172.31.0.34:2379
REMOVER=${REMOVER:-etcd3}
[ "$REMOVER" = lider ] && REMOVER=$(lider)
# executa o etcdctl num membro que não será removido
CTL_NO=etcd1; [ "$REMOVER" = etcd1 ] && CTL_NO=etcd2
c() { ctl "$CTL_NO" "$@"; }
id_de() { c member list | grep ", $1, " | cut -d, -f1; }
status() { c member list -w table; ctl_todos "$CTL_NO" endpoint status --cluster -w table 2>/dev/null || true; }
TMP=$(mktemp)
trap 'rm -f "$TMP"' EXIT

T_INICIO=$(agora)

etapa "0. Estado inicial (remover no fim: $REMOVER; líder atual: $(lider))"
status
pausa

etapa "1. member add $NOVO --learner"
T=$(agora)
c member add "$NOVO" --learner --peer-urls="$NOVO_PEER"
T_ADD=$(dif "$T" "$(agora)")
echo ">> learner registrado em $T_ADD s. Votantes continuam 3 => quórum = 2."
c member list -w table
pausa

etapa "2. Subindo o $NOVO e medindo o catch-up (learner alcançando o líder)"
# a sonda já observa o etcd4 antes de ele existir: mede (a) a subida até ele responder no termo
# atual e (b) o catch-up propriamente dito (APPLIED INDEX alcança o do líder, folga de 5 entradas
# por causa do escritor contínuo)
sonda_bg "$TMP" sincronizar "$NOVO" 120 5
esperar_pronto "$TMP"
docker compose --profile extra up -d "$NOVO" 2>&1 | grep -v -E '^ *(Container|Network)' || true
wait "$SONDA_PID"
J=$(grep '^{' "$TMP")
echo ">> sonda: $J"
T_SUBIDA=$(echo "$J" | sed -E 's/.*"reingresso_s": ([0-9.]+).*/\1/')
# o etcd só atende clientes DEPOIS de alcançar o líder, então o catch-up é medido pelos carimbos
# do próprio log do etcd4: "starting an etcd server" -> "ready to serve client requests"
echo ">> linha do tempo do $NOVO (carimbos do log do etcd):"
MARCOS=$(docker logs "$NOVO" 2>&1 | docker exec -i cliente python /scripts/marcos_log.py)
echo "$MARCOS"
T_CATCHUP_MS=$(echo "$MARCOS" | grep 'ready to serve' | sed -E 's/^ *\+ *([0-9]+) ms.*/\1/')
echo ">> subida (comando até o learner responder no termo atual): $T_SUBIDA s; boot + catch-up interno: $T_CATCHUP_MS ms"
echo ">> O learner NÃO atende leitura linearizável (esperado: 'rpc not supported for learner'):"
docker exec "$CTL_NO" etcdctl --endpoints="$NOVO_CLIENTE" get /config/modo 2>&1 | tail -1 || true
echo ">> ...mas atende leitura serializável (dado local):"
docker exec "$CTL_NO" etcdctl --endpoints="$NOVO_CLIENTE" get /config/modo --consistency=s --print-value-only 2>&1 | tail -1 || true
pausa

etapa "3. member promote $NOVO (learner -> votante)"
ID_NOVO=$(id_de "$NOVO")
T=$(agora)
until c member promote "$ID_NOVO" 2>/dev/null; do
  echo "   ainda não sincronizado o suficiente para promover; tentando de novo..."; sleep 0.5
done
T_PROMOTE=$(dif "$T" "$(agora)")
echo ">> promovido em $T_PROMOTE s. Agora 4 votantes => quórum = 3 (tolera só 1 falha, igual a 3 membros!)"
status
pausa

etapa "4. member remove $REMOVER"
ID_REM=$(id_de "$REMOVER")
ERA_LIDER=nao; [ "$(lider)" = "$REMOVER" ] && ERA_LIDER=sim
if [ "$ERA_LIDER" = sim ] && [ "${MOVER_LIDER:-0}" = 1 ]; then
  # transferência de liderança: o líder escolhe o sucessor e o manda iniciar a eleição já
  # (MsgTimeoutNow), em vez de os outros esperarem o election timeout depois da remoção
  T=$(agora)
  docker exec "$CTL_NO" etcdctl --endpoints="$(ep_cliente "$REMOVER")" move-leader "$ID_NOVO"
  echo ">> liderança transferida para $NOVO em $(dif "$T" "$(agora)") s; $REMOVER agora é seguidor"
fi
T=$(agora)
c member remove "$ID_REM"
T_REMOVE=$(dif "$T" "$(agora)")
docker stop "$REMOVER" >/dev/null
echo ">> removido em $T_REMOVE s (era líder: $ERA_LIDER) e contêiner parado. Voltamos a 3 votantes => quórum = 2."
sonda esperar-saudavel 30
status

etapa "Resumo"
echo "member add (learner) : $T_ADD s"
echo "subida do etcd4      : $T_SUBIDA s"
echo "catch-up do learner  : $T_CATCHUP_MS ms"
echo "promote              : $T_PROMOTE s"
echo "remove ($REMOVER)      : $T_REMOVE s (era líder: $ERA_LIDER; move-leader antes: ${MOVER_LIDER:-0})"
echo "total                : $(dif "$T_INICIO" "$(agora)") s"
echo "log salvo em $LOG"
CSV=resultados/e9_reconfig_tempos.csv
[ -f "$CSV" ] || echo "data,cenario,removido,era_lider,add_s,subida_s,catchup_ms,promote_s,remove_s" > "$CSV"
echo "$(date +%Y%m%d_%H%M%S),${CENARIO:-manual},$REMOVER,$ERA_LIDER,$T_ADD,$T_SUBIDA,$T_CATCHUP_MS,$T_PROMOTE,$T_REMOVE" >> "$CSV"
