#!/usr/bin/env bash
# Mostra membros, líder, termo e índice do Raft (E1).
docker exec etcd1 etcdctl --endpoints=http://127.0.0.1:2379 member list -w table 2>/dev/null \
  || docker exec etcd2 etcdctl --endpoints=http://127.0.0.1:2379 member list -w table
docker exec etcd1 etcdctl --endpoints=http://127.0.0.1:2379 endpoint status --cluster -w table 2>/dev/null \
  || docker exec etcd2 etcdctl --endpoints=http://127.0.0.1:2379 endpoint status --cluster -w table
