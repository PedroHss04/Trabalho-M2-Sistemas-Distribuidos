# Trabalho 2 – Sistemas Distribuídos – Tema 5 (etcd / Raft) – Ênfase C

**Configuração dinâmica e membros:** aplicações que reagem a mudanças de configuração via `watch`
e reconfiguração do cluster (learner → promote → remove) sem perda de disponibilidade.

## Topologia

| Contêiner | Papel | Rede interna (Raft, 2380) | Rede clientes (API, 2379) | Memória |
|---|---|---|---|---|
| etcd1 | membro etcd | 172.30.0.31 | 172.31.0.31 | 256 MB |
| etcd2 | membro etcd | 172.30.0.32 | 172.31.0.32 | 256 MB |
| etcd3 | membro etcd | 172.30.0.33 | 172.31.0.33 | 256 MB |
| etcd4 | 4º membro (profile `extra`, entra como learner) | 172.30.0.34 | 172.31.0.34 | 256 MB |
| app1..3 | aplicações que observam `/config/` | – | 172.31.0.101–103 | 64 MB |
| cliente | roda os scripts Python de medição | – | 172.31.0.100 | 64 MB |

Imagens: `quay.io/coreos/etcd:v3.5.17` e `python:3.12-alpine`. As apps usam só a biblioteca
padrão do Python (API v3 do etcd via gateway HTTP/JSON), então não há `pip install`.

## Subir do zero

Requisitos: Docker + Docker Compose e Git. No Windows, rode os `.sh` pelo terminal do **WSL**.

```bash
docker compose up -d
./scripts/reset.sh        # recria tudo, espera o cluster e grava a config inicial
./scripts/status.sh       # membros, líder, termo e índice do Raft
docker compose logs -f app1 app2 app3   # acompanhar as aplicações
```

## Estrutura

```
docker-compose.yml       cluster + apps + cliente
app/etcdhttp.py          cliente mínimo do etcd (put/get/watch com failover e retomada por revisão)
app/app_watch.py         aplicação que aplica a configuração a cada mudança e grava ACK em /acks/<app>
scripts/status.sh        E1: member list + endpoint status
scripts/reset.sh         recria o laboratório (usar antes de cada repetição)
scripts/medir_watch.py   E9 parte 1: atraso put -> app aplicou (média/desvio, CSV)
scripts/escritor.py      escritas contínuas: prova de disponibilidade durante a reconfiguração
scripts/carga.py         gera milhares de chaves para tornar o catch-up do learner mensurável
scripts/e9_reconfig.sh   E9 parte 2: learner -> catch-up -> promote -> remove (com tempos)
resultados/              CSVs e logs gerados (evidências para o relatório)
```

## E9 – Ênfase C (roteiro)

### Parte 1 – Configuração dinâmica via watch

```bash
docker compose logs -f app1 app2 app3                      # terminal 1
docker exec etcd1 etcdctl put /config/modo manutencao      # terminal 2: as 3 apps reagem na hora
docker exec cliente python /scripts/medir_watch.py 10      # medição (10 repetições, CSV em resultados/)
```

**Watch sobrevive à queda do líder:** com os logs abertos, derrube o membro em que as apps estão
conectadas (`docker stop etcd1`) e faça novos `put` por outro membro. Cada app mostra
`watch caiu ... retomando da revisão N` e depois aplica **todas** as mudanças, sem perder nenhuma.
Isso acontece porque o watch é retomado pela revisão (relógio lógico do etcd, ligação com a Q9).

### Parte 2 – Reconfiguração sem indisponibilidade

```bash
./scripts/reset.sh
docker exec cliente python /scripts/carga.py 5000          # opcional: catch-up mensurável
docker exec cliente python /scripts/escritor.py 180        # terminal 1: escritas a cada 50 ms
./scripts/e9_reconfig.sh                                   # terminal 2: ENTER entre as etapas
```

O script faz e cronometra:

1. `member add etcd4 --learner`: o learner é registrado, os votantes continuam 3 e o quórum continua 2.
2. Sobe o `etcd4` e mede o **catch-up** (RAFT APPLIED INDEX do learner alcança o do cluster).
   Ele mostra que o learner recusa leitura linearizável (`rpc not supported for learner`) e aceita a serializável.
3. `member promote`: 4 votantes, quórum 3. Ainda tolera só 1 falha (gancho para a S2).
4. `member remove etcd3`: volta a 3 votantes. Use `REMOVER=etcd1` para remover o líder e
   observar uma eleição durante a reconfiguração.

Ao final, o `escritor.py` mostra o total de falhas e o **maior intervalo sem escrita bem-sucedida**.
Essa é a evidência de "sem perda de disponibilidade".

**5 repetições (R7):**
```bash
for i in 1 2 3 4 5; do ./scripts/reset.sh >/dev/null; docker exec cliente python /scripts/carga.py 5000 >/dev/null; AUTO=1 ./scripts/e9_reconfig.sh; done
cat resultados/reconfig_tempos.csv     # CSV;catch-up;promote;remove por linha
```

## Referência rápida para E1–E8

```bash
EP=172.31.0.31:2379,172.31.0.32:2379,172.31.0.33:2379
docker exec etcd1 etcdctl --endpoints=$EP endpoint status -w table          # E1: líder, termo, índice
docker exec etcd2 etcdctl --endpoints=172.31.0.32:2379 put /x 1 -w json     # E2: put num seguidor
docker exec etcd3 etcdctl --endpoints=172.31.0.33:2379 watch /x             # E3: watch noutro membro
docker exec etcd1 etcdctl get /x --consistency=s                            # E4: leitura serializável
docker stop etcd1                                                           # E5: derrubar o líder
docker network disconnect t5-etcd-enfasec_interna etcd1                     # E6: isolar o líder (ver nome: docker network ls)
docker network connect --ip 172.30.0.31 t5-etcd-enfasec_interna etcd1       # E7: reconectar
docker exec etcd1 etcdctl lock trava                                        # E8: exclusão mútua
docker exec etcd2 etcdctl elect eleicao candidatoA                          # E8: eleição
```

Armadilhas: use **endpoint único** para observar um membro específico (com a lista completa, o
cliente troca de membro sozinho). `docker compose down` apaga os dados, então use `stop`/`start`
entre experimentos que dependem do estado.

## Limitações (para a seção 9 do relatório)

- Todos os contêineres compartilham o relógio do host. Os atrasos do `medir_watch.py` usam esse
  relógio comum, o que não seria possível em máquinas separadas sem sincronização.
- Rede local sem latência real; o catch-up depende do tamanho do banco (use `carga.py`).
- Sem TLS e sem autenticação (apenas laboratório).

## Status dos testes

Os scripts foram testados com binários etcd v3.5.17 reais rodando como processos (3 membros +
learner): watch, medição, reconfiguração completa com o escritor contínuo (0 falhas) e
retomada do watch após a queda do líder. A camada Docker (compose, redes e IPs) foi validada
com `docker compose config`, mas ainda não foi executada com os contêineres. Rode
`./scripts/reset.sh` na sua máquina como primeiro teste.
