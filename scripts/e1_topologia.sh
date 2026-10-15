#!/usr/bin/env bash
# E1 - Subida e topologia: membros, líder, termo/índice do Raft, saúde, endereços e memória.
. "$(dirname "$0")/lib.sh"
registrar e1_topologia

etapa "Versões"
docker exec etcd1 etcd --version | head -3
docker compose version

etapa "member list (quem faz parte do cluster)"
ctl etcd1 member list -w table

etapa "endpoint status (líder, termo e índice do Raft de cada membro)"
ctl_todos etcd1 endpoint status -w table

etapa "endpoint health"
ctl_todos etcd1 endpoint health -w table

etapa "Endereços por rede (interna = Raft/2380, clientes = API/2379)"
for c in etcd1 etcd2 etcd3 app1 app2 app3 cliente; do
  docker inspect -f '{{.Name}} {{range $n, $c := .NetworkSettings.Networks}} {{$n}}={{$c.IPAddress}}{{end}}' "$c" | sed 's#^/##'
done

etapa "Memória: limite declarado x uso real (R1)"
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.CPUPerc}}'
