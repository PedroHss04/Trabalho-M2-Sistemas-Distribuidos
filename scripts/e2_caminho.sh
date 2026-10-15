#!/usr/bin/env bash
# E2 - Caminho de uma operação: put num SEGUIDOR (endpoint único) é encaminhado ao líder,
# replicado (AppendEntries), confirmado pela maioria e só então respondido.
. "$(dirname "$0")/lib.sh"
registrar e2_caminho

L=$(lider)
F=$(for m in $MEMBROS; do [ "$m" != "$L" ] && echo "$m" && break; done)
echo "líder: $L   seguidor escolhido: $F"

etapa "Antes: RAFT INDEX dos três membros"
ctl_todos etcd1 endpoint status -w table

etapa "put em $F (endpoint único $(ep_cliente "$F")) com -w json"
ctl "$F" put /config/modo producao -w json
echo
echo ">> header.member_id = quem respondeu ($F); raft_term = termo do líder; revision = ordem global da escrita"

etapa "Depois: o RAFT INDEX avançou nos três (a entrada foi replicada e aplicada em todos)"
ctl_todos etcd1 endpoint status -w table

etapa "Leitura linearizável no mesmo seguidor (ReadIndex: o seguidor confirma com o líder)"
ctl "$F" get /config/modo -w json

etapa "Custo do salto extra: put direto no líder x via seguidor (100 de cada)"
sonda caminho 100
