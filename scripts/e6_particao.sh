#!/usr/bin/env bash
# E6 - Partição de rede: o LÍDER é desconectado da rede interna (Raft), mas continua
# acessível aos clientes. Observa os dois lados da partição e conclui C x A.
. "$(dirname "$0")/lib.sh"
registrar e6_particao
trap restaurar EXIT
T="--command-timeout=3s"
hora() { date +%H:%M:%S.%3N; }
TMP=$(mktemp)

L=$(lider)
M=$(for m in $MEMBROS; do [ "$m" != "$L" ] && echo "$m" && break; done)
echo "líder atual: $L (será isolado)   membro do lado majoritário usado nas consultas: $M"
ctl "$L" put /e6/valor ANTES-DA-PARTICAO >/dev/null

etapa "Antes da partição"
ctl_todos "$M" endpoint status -w table

etapa "Desconectando $L da rede interna"
sonda_bg "$TMP" eleicao 30
esperar_pronto "$TMP"
echo "$(hora) docker network disconnect $REDE_INTERNA $L"
docker network disconnect "$REDE_INTERNA" "$L"
wait "$SONDA_PID"
echo "$(hora) sonda: $(grep '^{' "$TMP")"

etapa "Logo após a eleição: as duas visões (o isolado pode ainda se achar líder por um instante)"
ctl_todos "$M" endpoint status -w table || true
sleep 3
etapa "3 s depois (check quorum: o líder isolado percebe que perdeu a maioria e renuncia)"
ctl_todos "$M" endpoint status -w table || true

etapa "Lado MAJORITÁRIO ($M e o outro membro)"
echo "$(hora) put /e6/valor=DEPOIS (via $M): $(ctl "$M" $T put /e6/valor DEPOIS-DA-PARTICAO 2>&1 | tail -1)"
echo "$(hora) get linearizável em $M:   $(ctl "$M" $T get /e6/valor --print-value-only 2>&1 | tail -1)"

etapa "Lado MINORITÁRIO: cliente conectado só a $L"
echo "$(hora) put em $L:                 $(ctl "$L" $T put /e6/valor DO-ISOLADO 2>&1 | tail -1)"
echo "$(hora) get linearizável em $L:   $(ctl "$L" $T get /e6/valor --print-value-only 2>&1 | tail -1)"
echo "$(hora) get serializável em $L:   $(ctl "$L" $T get /e6/valor --print-value-only --consistency=s 2>&1 | tail -1)"
echo
echo ">> Conclusão: o lado minoritário recusa escritas e leituras linearizáveis (sacrifica"
echo "   disponibilidade para não violar a consistência) => CP. Só a leitura serializável,"
echo "   que o cliente pede explicitamente abrindo mão da garantia, devolve o valor antigo."

etapa "Reconectando $L (recuperação: ver E7)"
sonda_bg "$TMP" sincronizar "$L" 60
esperar_pronto "$TMP"
docker network connect --ip "$(ip_peer "$L")" "$REDE_INTERNA" "$L"
wait "$SONDA_PID"
grep '^{' "$TMP"
ctl_todos "$M" endpoint status -w table
echo "$(hora) get linearizável em $L: $(ctl "$L" get /e6/valor --print-value-only)"
rm -f "$TMP"
