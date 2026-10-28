#!/usr/bin/env bash
# E4 - Consistência: leitura linearizável (padrão) x serializável.
#  1. Condições normais: mesmo valor? Latências diferentes? (200 leituras de cada)
#  2. Seguidor isolado: escreve pela maioria e lê no isolado com as duas consistências.
. "$(dirname "$0")/lib.sh"
registrar e4_consistencia
trap restaurar EXIT
T="--command-timeout=3s"
hora() { date +%H:%M:%S.%3N; }

L=$(lider)
F=$(for m in $MEMBROS; do [ "$m" != "$L" ] && echo "$m"; done | tail -1)
echo "líder: $L   seguidor que será isolado: $F"

etapa "1. Condições normais: latência de get linearizável x serializável"
sonda leituras 200 "$F"
sonda leituras 200 "$L"

etapa "2. Isolando $F da rede interna (continua acessível aos clientes)"
ctl "$L" put /e4/valor ANTIGO >/dev/null
sleep 0.5
echo "$(hora) $F lê (serializável): $(ctl "$F" get /e4/valor --print-value-only --consistency=s)"
docker network disconnect "$REDE_INTERNA" "$F"
echo "$(hora) $F desconectado da rede interna"
sleep 3

echo "$(hora) put /e4/valor=NOVO pela maioria ($L):"
ctl "$L" put /e4/valor NOVO
echo "$(hora) $L lê (linearizável): $(ctl "$L" get /e4/valor --print-value-only)"
echo "$(hora) $F lê (serializável):  $(ctl "$F" get /e4/valor --print-value-only --consistency=s 2>&1)"
echo "$(hora) $F lê (linearizável):  $(ctl "$F" $T get /e4/valor --print-value-only 2>&1 | tail -1)"
echo "$(hora) $F tenta escrever:     $(ctl "$F" $T put /e4/valor DO-ISOLADO 2>&1 | tail -1)"
echo
echo ">> visão do isolado (endpoint status dele mesmo):"
ctl "$F" endpoint status -w table

etapa "3. Reconectando $F: a leitura serializável passa a devolver o valor novo"
docker network connect --ip "$(ip_peer "$F")" "$REDE_INTERNA" "$F"
sonda sincronizar "$F" 30
echo "$(hora) $F lê (serializável):  $(ctl "$F" get /e4/valor --print-value-only --consistency=s)"
echo "$(hora) $F lê (linearizável):  $(ctl "$F" get /e4/valor --print-value-only)"
