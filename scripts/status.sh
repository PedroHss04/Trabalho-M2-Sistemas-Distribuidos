#!/usr/bin/env bash
# Mostra membros, líder, termo e índice do Raft, e a saúde de cada membro (E1).
. "$(dirname "$0")/lib.sh"
set +e
M=$(algum_vivo)
[ -n "$M" ] || { echo "nenhum membro etcd rodando"; exit 1; }
ctl "$M" member list -w table
ctl "$M" endpoint status --cluster -w table
ctl "$M" endpoint health --cluster -w table
